#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <cassert>
#include <poll.h>
#include <sys/timerfd.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/time.h>

#define SEND_BUFFER_SEGMENTS 8192
#define MIN_SEND_WINDOW 8
#define MAX_SEND_WINDOW 32
#define HANDSHAKE_TIMEOUT_US 100000

using namespace std;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

int sender_window_size = MIN_SEND_WINDOW;
long sender_timer_nsec = 20000000;

int send_data(int conn_id, char *buffer, int len)
{
    int size = 0;
    int ret;
    struct connection *con = cons[conn_id];

    pthread_mutex_lock(&con->con_lock);

    // Daca bufferul intern este plin, aplicatia incearca mai tarziu
    if (con->send_buffer_len == con->send_buffer_size)
    {
        pthread_mutex_unlock(&con->con_lock);
        return -1;
    }

    // Impartim datele primite in segmente de maxim MAX_DATA_SIZE
    while (size < len && con->send_buffer_len < con->send_buffer_size)
    {
        int pos = con->send_buffer_end;
        int payload_len = len - size;

        if (payload_len > MAX_DATA_SIZE)
            payload_len = MAX_DATA_SIZE;

        memset(&con->send_buffer[pos], 0, sizeof(struct send_segment));

        con->send_buffer[pos].len = payload_len;
        con->send_buffer[pos].seq = con->send_next_seq++;
        con->send_buffer[pos].sent = 0;

        memcpy(con->send_buffer[pos].data, buffer + size, payload_len);

        con->send_buffer_end = (con->send_buffer_end + 1) % con->send_buffer_size;

        con->send_buffer_len++;
        size += payload_len;
    }

    // Trimitem segmente noi cat timp mai avem loc in fereastra
    while (con->send_buffer_len > con->send_in_flight &&
           con->send_in_flight < con->send_window_size)
    {
        int pos = (con->send_buffer_start + con->send_in_flight) %
                  con->send_buffer_size;
        int used_window = 0;

        // Verificam si cati bytes sunt deja in zbor
        for (int i = 0; i < con->send_in_flight; i++)
        {
            int old_pos = (con->send_buffer_start + i) %
                          con->send_buffer_size;

            used_window += con->send_buffer[old_pos].len;
        }

        // Nu trimitem mai mult decat poate primi receiverul
        if (used_window + con->send_buffer[pos].len > con->recv_window)
            break;

        char packet[MAX_SEGMENT_SIZE];
        struct poli_tcp_data_hdr hdr;
        memset(&hdr, 0, sizeof(hdr));

        hdr.protocol_id = POLI_PROTOCOL_ID;
        hdr.conn_id = con->conn_id;
        hdr.type = TYPE_DATA;
        hdr.seq_num = con->send_buffer[pos].seq;
        hdr.len = con->send_buffer[pos].len;

        // Punem header-ul si payload-ul in acelasi buffer
        memcpy(packet, &hdr, sizeof(hdr));
        memcpy(packet + sizeof(hdr),
               con->send_buffer[pos].data,
               con->send_buffer[pos].len);

        ret = sendto(con->sockfd, packet,
                     sizeof(hdr) + con->send_buffer[pos].len, 0,
                     (struct sockaddr *)&con->servaddr,
                     sizeof(con->servaddr));
        assert(ret >= 0);

        con->send_buffer[pos].sent = 1;
        con->send_in_flight++;
    }

    pthread_mutex_unlock(&con->con_lock);

    return size;
}

void *sender_handler(void *arg)
{
    int res = 0;
    char buf[MAX_SEGMENT_SIZE];

    while (1)
    {

        if (cons.size() == 0)
        {
            continue;
        }

        int conn_id = -1;
        do
        {
            res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);
        } while (res == -14);

        pthread_mutex_lock(&cons[conn_id]->con_lock);
        struct connection *con = cons[conn_id];

        if (res == -1)
        {
            // La timeout retrimitem toata fereastra curenta
            for (int i = 0; i < con->send_in_flight; i++)
            {
                int pos = (con->send_buffer_start + i) %
                          con->send_buffer_size;

                if (con->send_buffer[pos].sent)
                {
                    char packet[MAX_SEGMENT_SIZE];
                    struct poli_tcp_data_hdr hdr;
                    memset(&hdr, 0, sizeof(hdr));

                    hdr.protocol_id = POLI_PROTOCOL_ID;
                    hdr.conn_id = con->conn_id;
                    hdr.type = TYPE_DATA;
                    hdr.seq_num = con->send_buffer[pos].seq;
                    hdr.len = con->send_buffer[pos].len;

                    // Refacem segmentul din bufferul de trimitere
                    memcpy(packet, &hdr, sizeof(hdr));
                    memcpy(packet + sizeof(hdr),
                           con->send_buffer[pos].data,
                           con->send_buffer[pos].len);

                    res = sendto(con->sockfd, packet,
                                 sizeof(hdr) + con->send_buffer[pos].len, 0,
                                 (struct sockaddr *)&con->servaddr,
                                 sizeof(con->servaddr));
                    assert(res >= 0);
                }
            }
        }
        else if (res >= (int)sizeof(struct poli_tcp_ctrl_hdr))
        {
            struct poli_tcp_ctrl_hdr *ack = (struct poli_tcp_ctrl_hdr *)buf;

            if (ack->protocol_id == POLI_PROTOCOL_ID &&
                ack->type == TYPE_ACK &&
                ack->conn_id == conn_id)
            {
                int ack_pos = -1;

                con->recv_window = ack->recv_window;

                // Cautam ACK-ul in fereastra curenta
                for (int i = 0; i < con->send_buffer_len; i++)
                {
                    int pos = (con->send_buffer_start + i) %
                              con->send_buffer_size;

                    if (con->send_buffer[pos].seq == ack->ack_num)
                    {
                        ack_pos = i;
                        break;
                    }
                }

                // Scoatem toate segmentele confirmate
                if (ack_pos != -1 && ack_pos < con->send_in_flight)
                {
                    for (int i = 0; i <= ack_pos; i++)
                    {
                        con->send_buffer_start =
                            (con->send_buffer_start + 1) %
                            con->send_buffer_size;

                        con->send_buffer_len--;
                        con->send_in_flight--;
                    }
                }

                // Dupa ce se elibereaza loc, trimitem alte segmente
                while (con->send_buffer_len > con->send_in_flight &&
                       con->send_in_flight < con->send_window_size)
                {
                    int pos = (con->send_buffer_start + con->send_in_flight) %
                              con->send_buffer_size;
                    int used_window = 0;

                    // Verificam si fereastra de receptie
                    for (int i = 0; i < con->send_in_flight; i++)
                    {
                        int old_pos = (con->send_buffer_start + i) %
                                      con->send_buffer_size;

                        used_window += con->send_buffer[old_pos].len;
                    }

                    if (used_window + con->send_buffer[pos].len >
                        con->recv_window)
                        break;

                    char packet[MAX_SEGMENT_SIZE];
                    struct poli_tcp_data_hdr hdr;
                    memset(&hdr, 0, sizeof(hdr));

                    hdr.protocol_id = POLI_PROTOCOL_ID;
                    hdr.conn_id = con->conn_id;
                    hdr.type = TYPE_DATA;
                    hdr.seq_num = con->send_buffer[pos].seq;
                    hdr.len = con->send_buffer[pos].len;

                    // Punem header-ul si payload-ul in acelasi buffer
                    memcpy(packet, &hdr, sizeof(hdr));
                    memcpy(packet + sizeof(hdr),
                           con->send_buffer[pos].data,
                           con->send_buffer[pos].len);

                    res = sendto(con->sockfd, packet,
                                 sizeof(hdr) + con->send_buffer[pos].len, 0,
                                 (struct sockaddr *)&con->servaddr,
                                 sizeof(con->servaddr));
                    assert(res >= 0);

                    con->send_buffer[pos].sent = 1;
                    con->send_in_flight++;
                }
            }
        }

        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }
}

int setup_connection(uint32_t ip, uint16_t port)
{
    /* Implement the sender part of the Three Way Handshake. Blocks
    until the connection is established */

    int ret;
    int conn_id = 0;

    struct connection *con = (struct connection *)malloc(sizeof(struct connection));
    assert(con != NULL);

    memset(con, 0, sizeof(struct connection));

    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    assert(con->sockfd >= 0);

    // Setam timeout pentru handshake
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = HANDSHAKE_TIMEOUT_US;
    ret = setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    assert(ret >= 0);

    // Pregatim adresa serverului pe portul de conectare
    memset(&con->servaddr, 0, sizeof(con->servaddr));
    con->servaddr.sin_family = AF_INET;
    con->servaddr.sin_addr.s_addr = ip;
    con->servaddr.sin_port = port;

    struct poli_tcp_ctrl_hdr syn;
    memset(&syn, 0, sizeof(syn));

    syn.protocol_id = POLI_PROTOCOL_ID;
    syn.type = TYPE_SYN;

    char syn_ack[sizeof(struct poli_tcp_ctrl_hdr) + sizeof(uint16_t)];
    struct poli_tcp_ctrl_hdr syn_ack_hdr;
    uint16_t data_port;

    // Trimitem SYN pana primim SYN-ACK
    while (1)
    {
        ret = sendto(con->sockfd, &syn, sizeof(syn), 0,
                     (struct sockaddr *)&con->servaddr,
                     sizeof(con->servaddr));
        assert(ret >= 0);

        // Socketul are timeout, deci recvfrom nu blocheaza la infinit
        ret = recvfrom(con->sockfd, syn_ack, sizeof(syn_ack), 0, NULL, NULL);

        if (ret < 0)
        {
            // Daca s-a pierdut SYN-ul sau SYN-ACK-ul, retrimitem SYN
            if (errno == EWOULDBLOCK || errno == EAGAIN)
                continue;

            assert(ret >= 0);
        }

        // Ignoram raspunsurile incomplete
        if (ret < (int)sizeof(syn_ack))
            continue;

        // Extragem header-ul din bufferul primit
        memcpy(&syn_ack_hdr, syn_ack, sizeof(syn_ack_hdr));

        if (syn_ack_hdr.protocol_id == POLI_PROTOCOL_ID &&
            syn_ack_hdr.type == TYPE_SYN_ACK)
        {
            // Dupa header, serverul trimite portul dedicat conexiunii
            memcpy(&data_port,
                   syn_ack + sizeof(struct poli_tcp_ctrl_hdr),
                   sizeof(data_port));

            conn_id = syn_ack_hdr.conn_id;
            con->conn_id = conn_id;
            con->recv_window = syn_ack_hdr.recv_window;
            con->servaddr.sin_port = data_port;
            break;
        }
    }

    // Trimitem ACK-ul final pe portul dedicat conexiunii
    struct poli_tcp_ctrl_hdr ack;
    memset(&ack, 0, sizeof(ack));

    ack.protocol_id = POLI_PROTOCOL_ID;
    ack.conn_id = conn_id;
    ack.type = TYPE_ACK;

    for (int i = 0; i < 3; i++)
    {
        ret = sendto(con->sockfd, &ack, sizeof(ack), 0,
                     (struct sockaddr *)&con->servaddr,
                     sizeof(con->servaddr));
        assert(ret >= 0);

        usleep(1000);
    }

    // Initializam bufferul intern de trimitere
    con->send_buffer_size = SEND_BUFFER_SEGMENTS;
    con->send_buffer = (struct send_segment *)malloc(sizeof(struct send_segment) *
                                                     con->send_buffer_size);
    assert(con->send_buffer != NULL);
    memset(con->send_buffer, 0, sizeof(struct send_segment) * con->send_buffer_size);

    con->send_buffer_start = 0;
    con->send_buffer_end = 0;
    con->send_buffer_len = 0;
    con->send_in_flight = 0;
    con->send_next_seq = 0;
    con->send_window_size = sender_window_size;

    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */
    data_fds[fdmax].fd = con->sockfd;
    data_fds[fdmax].events = POLLIN;

    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on our connection */
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME, 0);
    assert(timer_fds[fdmax].fd >= 0);

    timer_fds[fdmax].events = POLLIN;
    struct itimerspec spec;
    spec.it_value.tv_sec = 0;
    spec.it_value.tv_nsec = sender_timer_nsec;
    spec.it_interval.tv_sec = 0;
    spec.it_interval.tv_nsec = sender_timer_nsec;

    ret = timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);
    assert(ret >= 0);

    pthread_mutex_init(&con->con_lock, NULL);
    cons.insert({conn_id, con});
    fdmax++;

    DEBUG_PRINT("Connection established!");

    return conn_id;
}

void init_sender(int speed, int delay)
{
    pthread_t thread1;
    int ret;
    int timeout_ms;

    // Calculam o fereastra simpla folosind speed si delay
    sender_window_size = (speed * delay * 250) / MAX_DATA_SIZE + 1;

    if (sender_window_size < MIN_SEND_WINDOW)
        sender_window_size = MIN_SEND_WINDOW;

    if (sender_window_size > MAX_SEND_WINDOW)
        sender_window_size = MAX_SEND_WINDOW;

    timeout_ms = TIMEOUT_SEND(delay);

    if (timeout_ms < 10)
        timeout_ms = 10;

    if (timeout_ms > 20)
        timeout_ms = 20;

    sender_timer_nsec = timeout_ms * 1000000L;

    /* Create a thread that will*/
    ret = pthread_create(&thread1, NULL, sender_handler, NULL);
    assert(ret == 0);
}
