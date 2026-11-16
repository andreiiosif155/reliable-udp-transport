#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <poll.h>
#include <cassert>
#include <sys/timerfd.h>
#include <string.h>
#include <errno.h>
#include <sys/time.h>

#define CONNECTION_PORT 8032

using namespace std;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

int listening_socket = -1;
int next_conn_id = 0;
int receiver_buffer_size = 0;

int recv_data(int conn_id, char *buffer, int len)
{
    int size = 0;
    int ret;

    // Asteptam pana cand exista date in bufferul intern al conexiunii
    while (1)
    {
        pthread_mutex_lock(&cons[conn_id]->con_lock);

        // Daca avem date disponibile, putem incepe sa copiem catre utilizator
        if (cons[conn_id]->recv_buffer_len > 0)
            break;

        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }

    // Copiem maxim len bytes, dar nu mai mult decat avem deja primit
    if (len < cons[conn_id]->recv_buffer_len)
        size = len;
    else
        size = cons[conn_id]->recv_buffer_len;

    memcpy(buffer, cons[conn_id]->recv_buffer, size);

    // Mutam la inceput datele ramase in bufferul intern
    memmove(cons[conn_id]->recv_buffer,
            cons[conn_id]->recv_buffer + size,
            cons[conn_id]->recv_buffer_len - size);

    cons[conn_id]->recv_buffer_len -= size;

    // Dupa ce eliberam spatiu, anuntam senderul de fereastra noua
    if (cons[conn_id]->expected_seq > 0)
    {
        struct poli_tcp_ctrl_hdr ack;
        memset(&ack, 0, sizeof(ack));

        ack.protocol_id = POLI_PROTOCOL_ID;
        ack.conn_id = conn_id;
        ack.type = TYPE_ACK;
        ack.ack_num = cons[conn_id]->expected_seq - 1;
        ack.recv_window = cons[conn_id]->recv_buffer_size -
                          cons[conn_id]->recv_buffer_len;

        ret = sendto(cons[conn_id]->sockfd, &ack, sizeof(ack), 0,
                     (struct sockaddr *)&cons[conn_id]->servaddr,
                     sizeof(cons[conn_id]->servaddr));
        assert(ret >= 0);
    }

    pthread_mutex_unlock(&cons[conn_id]->con_lock);

    return size;
}

void *receiver_handler(void *arg)
{

    char segment[MAX_SEGMENT_SIZE];
    int res;
    int ret;
    DEBUG_PRINT("Starting recviver handler\n");

    while (1)
    {

        int conn_id = -1;
        do
        {
            res = recv_message_or_timeout(segment, MAX_SEGMENT_SIZE, &conn_id);
        } while (res == -14);

        if (res == -1)
            continue;

        pthread_mutex_lock(&cons[conn_id]->con_lock);

        struct poli_tcp_data_hdr *hdr = (struct poli_tcp_data_hdr *)segment;

        if (hdr->protocol_id == POLI_PROTOCOL_ID && hdr->type == TYPE_DATA)
        {
            char *payload = segment + sizeof(struct poli_tcp_data_hdr);
            int payload_len = hdr->len;
            int ack_num;

            // Acceptam doar segmentul pe care il asteptam
            if (hdr->seq_num == cons[conn_id]->expected_seq)
            {
                int free_space = cons[conn_id]->recv_buffer_size -
                                 cons[conn_id]->recv_buffer_len;

                if (payload_len > free_space)
                {
                    // Nu avem loc, deci anuntam fereastra ramasa
                    ack_num = cons[conn_id]->expected_seq - 1;
                }
                else
                {
                    // Punem payload-ul la finalul bufferului intern al conexiunii
                    memcpy(cons[conn_id]->recv_buffer + cons[conn_id]->recv_buffer_len,
                           payload,
                           payload_len);

                    cons[conn_id]->recv_buffer_len += payload_len;
                    cons[conn_id]->expected_seq++;
                    ack_num = hdr->seq_num;
                }
            }
            else
            {
                // Pentru duplicate trimitem ultimul ACK bun
                ack_num = cons[conn_id]->expected_seq - 1;
            }

            // Trimitem ACK pentru segmentul primit
            struct poli_tcp_ctrl_hdr ack;
            memset(&ack, 0, sizeof(ack));

            ack.protocol_id = POLI_PROTOCOL_ID;
            ack.conn_id = conn_id;
            ack.type = TYPE_ACK;
            ack.ack_num = ack_num;
            ack.recv_window = cons[conn_id]->recv_buffer_size -
                              cons[conn_id]->recv_buffer_len;

            ret = sendto(cons[conn_id]->sockfd, &ack, sizeof(ack), 0,
                         (struct sockaddr *)&cons[conn_id]->servaddr,
                         sizeof(cons[conn_id]->servaddr));
            assert(ret >= 0);
        }

        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }
}

int wait4connect(uint32_t ip, uint16_t port)
{
    // Header-ul SYN primit de la client
    struct poli_tcp_ctrl_hdr syn;

    // Adresa clientului de la care primim SYN-ul
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    int ret;

    // Asteptam pana primim un pachet SYN valid pe socketul de ascultare
    while (1)
    {
        int duplicate = 0;

        ret = recvfrom(listening_socket, &syn, sizeof(syn), 0,
              (struct sockaddr *)&client_addr, &client_len);
        assert(ret >= 0);

        // Ignoram pachetele care nu apartin protocolului nostru
        // sau care nu sunt de tip SYN
        if (syn.protocol_id != POLI_PROTOCOL_ID || syn.type != TYPE_SYN)
            continue;

        // Ignoram SYN-uri vechi de la clienti deja conectati
        for (auto const &x : cons)
        {
            if (x.second->servaddr.sin_addr.s_addr == client_addr.sin_addr.s_addr &&
                x.second->servaddr.sin_port == client_addr.sin_port)
            {
                duplicate = 1;
                break;
            }
        }

        if (!duplicate)
            break;
    }

    // Cream un socket nou, dedicat acestei conexiuni
    // Pe acesta vom primi datele
    int data_socket = socket(AF_INET, SOCK_DGRAM, 0);
    assert(data_socket >= 0);

    // Setam timeout pentru ACK-ul final din handshake
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 100000;
    ret = setsockopt(data_socket, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    assert(ret >= 0);

    // Pregatim adresa locala pentru socketul de date
    struct sockaddr_in data_addr;
    memset(&data_addr, 0, sizeof(data_addr));

    // Folosim IPv4
    data_addr.sin_family = AF_INET;

    // Ascultam pe orice adresa IP locala a serverului
    data_addr.sin_addr.s_addr = INADDR_ANY;

    // Punem portul 0 ca sa lasam OS sa aleaga
    // automat un port liber pentru aceasta conexiune
    data_addr.sin_port = htons(0);

    // Legam socketul de conexiune la portul deschis pentru date
    ret = bind(data_socket, (struct sockaddr *)&data_addr, sizeof(data_addr));
    assert(ret >= 0);

    // Dupa bind, aflam ce port a ales sistemul de operare
    socklen_t data_len = sizeof(data_addr);
    ret = getsockname(data_socket, (struct sockaddr *)&data_addr, &data_len);
    assert(ret >= 0);

    // Salvam portul pe care il vom trimite clientului in SYN-ACK
    uint16_t data_port = data_addr.sin_port;

    struct connection *con = (struct connection *)malloc(sizeof(struct connection));
    assert(con != NULL);

    int conn_id = next_conn_id++;
    assert(conn_id < MAX_CONNECTIONS);

    memset(con, 0, sizeof(struct connection));
    con->sockfd = data_socket;
    con->conn_id = conn_id;

    // Alocam bufferul intern in care receiver_handler va pune datele primite
    con->recv_buffer_size = receiver_buffer_size;
    con->recv_buffer = (char *)malloc(con->recv_buffer_size);
    assert(con->recv_buffer != NULL);

    // Initial bufferul este gol
    con->recv_buffer_len = 0;
    con->expected_seq = 0;

    // Pregatim bufferul pentru SYN-ACK
    // header control + portul socketului dedicat
    char syn_ack[sizeof(struct poli_tcp_ctrl_hdr) + sizeof(uint16_t)];
    int offset = 0;

    memset(syn_ack, 0, sizeof(syn_ack));

    // Adaugam campurile din header in buffer
    syn_ack[offset++] = POLI_PROTOCOL_ID;
    syn_ack[offset++] = conn_id;
    syn_ack[offset++] = TYPE_SYN_ACK;

    uint16_t ack_num = 0;
    memcpy(syn_ack + offset, &ack_num, sizeof(ack_num));
    offset += sizeof(ack_num);

    uint16_t recv_window = receiver_buffer_size;

    memcpy(syn_ack + offset, &recv_window, sizeof(recv_window));
    offset += sizeof(recv_window);

    memcpy(syn_ack + offset, &data_port, sizeof(data_port));
    offset += sizeof(data_port);

    // Trimitem SYN-ACK-ul inapoi catre client
    ret = sendto(listening_socket, syn_ack, offset, 0,
          (struct sockaddr *)&client_addr, client_len);
    assert(ret >= 0);

    // Asteptam ACK-ul final de la client
    struct poli_tcp_ctrl_hdr ack;
    struct sockaddr_in ack_addr;
    socklen_t ack_len = sizeof(ack_addr);

    while (1)
    {
        ret = recvfrom(data_socket, &ack, sizeof(ack), 0,
              (struct sockaddr *)&ack_addr, &ack_len);

        if (ret < 0)
        {
            if (errno == EWOULDBLOCK || errno == EAGAIN)
            {
                // Daca ACK-ul s-a pierdut, retrimitem SYN-ACK-ul
                ret = sendto(listening_socket, syn_ack, offset, 0,
                             (struct sockaddr *)&client_addr, client_len);
                assert(ret >= 0);
                continue;
            }

            assert(ret >= 0);
        }

        if (ack.protocol_id == POLI_PROTOCOL_ID &&
            ack.type == TYPE_ACK &&
            ack.conn_id == conn_id)
        {
            break;
        }
    }

    memcpy(&con->servaddr, &ack_addr, sizeof(ack_addr));

    /* This can be used to set a timer on a socket, useful once we received a
     * SYN. You may want to disable by setting the time to 0 (tv_sec = 0,
     * tv_usec = 0)
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 100000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    } */

    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */
    data_fds[fdmax].fd = con->sockfd;
    data_fds[fdmax].events = POLLIN;

    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on a connection */
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME, 0);
    assert(timer_fds[fdmax].fd >= 0);

    timer_fds[fdmax].events = POLLIN;
    struct itimerspec spec;
    spec.it_value.tv_sec = 1;
    spec.it_value.tv_nsec = 0;
    spec.it_interval.tv_sec = 1;
    spec.it_interval.tv_nsec = 0;
    ret = timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);
    assert(ret >= 0);

    pthread_mutex_init(&con->con_lock, NULL);
    cons.insert({conn_id, con});
    fdmax++;

    DEBUG_PRINT("Connection established!");

    return conn_id;
}

void init_receiver(int recv_buffer_bytes)
{
    // Adresa serverului
    struct sockaddr_in local_addr;

    pthread_t thread1;
    int ret;

    receiver_buffer_size = recv_buffer_bytes;

    // Creeam socketul UDP
    listening_socket = socket(AF_INET, SOCK_DGRAM, 0); // IPv4, UDP
    assert(listening_socket >= 0);

    // Pregatim structura pentru completare deoarece nu ii vom completa
    // toate campurile
    memset(&local_addr, 0, sizeof(local_addr));

    // ascult pe orice IP local al serverului,
    // adresa IPv4 pe portul CONNECTION_PORT (8032)
    local_addr.sin_addr.s_addr = INADDR_ANY;
    local_addr.sin_family = AF_INET;
    local_addr.sin_port = htons(CONNECTION_PORT);

    // Facem bind pe socketul pe care vom primi primul SYN cu portul de ascultare
    ret = bind(listening_socket, (struct sockaddr *)&local_addr, sizeof(local_addr));
    assert(ret >= 0);

    ret = pthread_create(&thread1, NULL, receiver_handler, NULL);
    assert(ret == 0);
}
