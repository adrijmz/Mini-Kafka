#include "comun.h"
#include "kaska.h"
#include "map.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>

int s = -1;
struct map *mapa;
int err = 0;
map_position *pos;
map_iter *it;

typedef struct my_entry{
    char *tema;
    int offset;
}my_entry;

// inicializa el socket y se conecta al servidor
static int init_socket_client() {
    int s;
    const char *host_server = getenv("BROKER_HOST");
    const char *port = getenv("BROKER_PORT");
    struct addrinfo *res;
    // socket stream para Internet: TCP
    if ((s=socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
        perror("error creando socket");
        return -1;
    }

    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;    /* solo IPv4 */
    hints.ai_socktype = SOCK_STREAM;

    // obtiene la dirección TCP remota
    if (getaddrinfo(host_server, port, &hints, &res)!=0) {
        perror("error en getaddrinfo");
        close(s);
        return -1;
    }
    // realiza la conexión
    if (connect(s, res->ai_addr, res->ai_addrlen) < 0) {
        perror("error en connect");
        close(s);
        return -1;
    }
    freeaddrinfo(res);
    return s;
}

// Crea el tema especificado.
// Devuelve 0 si OK y un valor negativo en caso de error.
int create_topic(char *topic) {
    
    if(s==-1){    
        if ((s=init_socket_client()) < 0){
            perror("error en init_socket_server");
            return -1;
        } 
    }

    struct iovec iov[3];

    // envio de codigo de operacion
    int nelem = 0;
    int entero_net = htonl(0);
    iov[nelem].iov_base=&entero_net;
    iov[nelem++].iov_len=sizeof(int);

    // envio de string
    int longitud_str = strlen(topic);
    int longitud_str_net = htonl(longitud_str);
    iov[nelem].iov_base=&longitud_str_net;
    iov[nelem++].iov_len=sizeof(int);
    iov[nelem].iov_base=topic;
    iov[nelem++].iov_len=longitud_str;

    if(writev(s,iov,3)<0){
        perror("error en writev");
        close(s);
        return -1;
    }
    int res;

    if(recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int)){
        perror("error en recv aqui");
        close(s);
        return -1;
    }
    return ntohl(res);
}
// Devuelve cuántos temas existen en el sistema y un valor negativo
// en caso de error.
int ntopics(void) {

    if(s==-1){    
        if ((s=init_socket_client()) < 0){
            perror("error en init_socket_server");
            return -1;
        } 
    } 

    // envio de codigo de operacion
    struct iovec iov[1];

    int nelem = 0;
    int entero_net = htonl(1);
    iov[nelem].iov_base=&entero_net;
    iov[nelem++].iov_len=sizeof(int);

    if(writev(s,iov,1)<0){
        perror("error en writev");
        close(s);
        return -1;
    }

    int res;

    if(recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int)){
        perror("error en recv");
        close(s);
        return -1;
    }

    return ntohl(res);

}

// SEGUNDA FASE: PRODUCIR/PUBLICAR

// Envía el mensaje al tema especificado; nótese la necesidad
// de indicar el tamaño ya que puede tener un contenido de tipo binario.
// Devuelve el offset si OK y un valor negativo en caso de error.
int send_msg(char *topic, int msg_size, void *msg) {

    int offset = -1;

    if(s==-1){    
        if ((s=init_socket_client()) < 0){
            perror("error en init_socket_server");
            return -1;
        } 
    }

    struct iovec iov[5];

    // envio de codigo de operacion
    int nelem = 0;
    int entero_net = htonl(2);
    iov[nelem].iov_base=&entero_net;
    iov[nelem++].iov_len=sizeof(int);

    // envio de string
    int longitud_str = strlen(topic);
    int longitud_str_net = htonl(longitud_str);
    iov[nelem].iov_base=&longitud_str_net;
    iov[nelem++].iov_len=sizeof(int);
    iov[nelem].iov_base=topic;
    iov[nelem++].iov_len=longitud_str;

    // envio de tamaño del mensaje
    int longitud_msg_net = htonl(msg_size);
    iov[nelem].iov_base=&longitud_msg_net;
    iov[nelem++].iov_len=sizeof(int);

    // envio de mensaje
    iov[nelem].iov_base=msg;
    iov[nelem++].iov_len=msg_size;

    if(writev(s,iov,5)<0){
        perror("error en writev");
        close(s);
        return -1;
    }

    if(recv(s, &offset, sizeof(int), MSG_WAITALL) != sizeof(int)){
        perror("error en recv");
        close(s);
        return -1;
    }

    offset = ntohl(offset);

    return offset;
}
// Devuelve la longitud del mensaje almacenado en ese offset del tema indicado
// y un valor negativo en caso de error.
int msg_length(char *topic, int offset) {

    if(s==-1){    
        if ((s=init_socket_client()) < 0){
            perror("error en init_socket_server");
            return -1;
        } 
    }

    struct iovec iov[4];

    // envio de codigo de operacion
    int nelem = 0;
    int entero_net = htonl(3);
    iov[nelem].iov_base=&entero_net;
    iov[nelem++].iov_len=sizeof(int);

    // envio de string
    int longitud_str = strlen(topic);
    int longitud_str_net = htonl(longitud_str);
    iov[nelem].iov_base=&longitud_str_net;
    iov[nelem++].iov_len=sizeof(int);
    iov[nelem].iov_base=topic;
    iov[nelem++].iov_len=longitud_str;

    // envio de offset
    int offset_net = htonl(offset);
    iov[nelem].iov_base=&offset_net;
    iov[nelem++].iov_len=sizeof(int);

    if(writev(s,iov,4)<0){
        perror("error en writev");
        close(s);
        return -1;
    }

    int res;

    if(recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int)){
        perror("error en recv");
        close(s);
        return -1;
    }

    res = ntohl(res);

    return res;
}
// Obtiene el último offset asociado a un tema en el broker, que corresponde
// al del último mensaje enviado más uno y, dado que los mensajes se
// numeran desde 0, coincide con el número de mensajes asociados a ese tema.
// Devuelve ese offset si OK y un valor negativo en caso de error.
int end_offset(char *topic) {
    if(s==-1){    
        if ((s=init_socket_client()) < 0){
            perror("error en init_socket_server");
            return -1;
        } 
    }

    struct iovec iov[3];

    // envio de codigo de operacion
    int nelem = 0;
    int entero_net = htonl(4);
    iov[nelem].iov_base=&entero_net;
    iov[nelem++].iov_len=sizeof(int);

    // envio de topic
    int longitud_str = strlen(topic);
    int longitud_str_net = htonl(longitud_str);
    iov[nelem].iov_base=&longitud_str_net;
    iov[nelem++].iov_len=sizeof(int);
    iov[nelem].iov_base=topic;
    iov[nelem++].iov_len=longitud_str;

    if(writev(s,iov,3)<0){
        perror("error en writev");
        close(s);
        return -1;
    }
    int res;

    if(recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int)){
        perror("error en recv aqui");
        close(s);
        return -1;
    }
    return ntohl(res);
}

// TERCERA FASE: SUBSCRIPCIÓN


// Se suscribe al conjunto de temas recibidos. No permite suscripción
// incremental: hay que especificar todos los temas de una vez.
// Si un tema no existe o está repetido en la lista simplemente se ignora.
// Devuelve el número de temas a los que realmente se ha suscrito
// y un valor negativo solo si ya estaba suscrito a algún tema.
int subscribe(int ntopics, char **topics) {
    // comprueba si mapa es null
    if(mapa!=NULL)
        return -1;
    
    mapa = map_create(key_string,0);

    int i;
    int topics_suscritos = 0;
    for(i=0;i<ntopics;i++){
        int offset = end_offset(topics[i]);
        if(offset!=-1 && map_get(mapa, topics[i], &err)==NULL){
            char *topic = strdup(topics[i]);
            my_entry *e = malloc(sizeof(my_entry));
            e->tema = topic;
            e->offset = offset;
            map_put(mapa, topic, e);
            topics_suscritos++;
            printf("Suscrito al topic %s con offset %d\n", topic, offset);
        }
    }

    pos = map_alloc_position(mapa);

    if(topics_suscritos==0)
        return -1;

    return topics_suscritos;
    
}

// Se da de baja de todos los temas suscritos.
// Devuelve 0 si OK y un valor negativo si no había suscripciones activas.
int unsubscribe(void) {
    if(mapa==NULL)
        return -1;
    map_destroy(mapa, NULL);
    mapa = NULL;
    pos = NULL;
    return 0;
}

// Devuelve el offset del cliente para ese tema y un número negativo en
// caso de error.
int position(char *topic) {
    if(mapa==NULL)
        return -1;

    my_entry *e = map_get(mapa, topic, &err);
    if(e==NULL)
        return -1;

    //devuelve el numero de esa posicion de memoria
    return e->offset;
}

// Modifica el offset del cliente para ese tema.
// Devuelve 0 si OK y un número negativo en caso de error.
int seek(char *topic, int offset) {
    if(mapa==NULL)
        return -1;

    my_entry *e = map_get(mapa, topic, &err);
    if(e==NULL)
        return -1;
    e->offset = offset;
    return 0;
}

// CUARTA FASE: LEER MENSAJES

// Obtiene el siguiente mensaje destinado a este cliente; los dos parámetros
// son de salida.
// Devuelve el tamaño del mensaje (0 si no había mensaje)
// y un número negativo en caso de error.
int poll(char **topic, void **msg) {
    if(mapa==NULL)
        return -1;

    struct iovec iov[4];
    int longitud = 0;

    // envio de codigo de operacion
    int nelem = 0;
    int entero_net = htonl(5);
    iov[0].iov_base=&entero_net;
    iov[0].iov_len=sizeof(int);

    it = map_iter_init(mapa, pos);
    

    while(longitud==0 && map_iter_has_next(it)){
        
        // map_iter_next(it);

        // obtiene el primer elemento del mapa
        char *key;
        my_entry *e;

        map_iter_value(it, &key, &e);

        //envio de topic
        int longitud_str = strlen(e->tema);
        int longitud_str_net = htonl(longitud_str);
        iov[1].iov_base=&longitud_str_net;
        iov[1].iov_len=sizeof(int);
        iov[2].iov_base=e->tema;
        iov[2].iov_len=longitud_str;

        //envio de offset
        int offset_net = htonl(e->offset);
        iov[3].iov_base=&offset_net;
        iov[3].iov_len=sizeof(int);
        
        if(writev(s,iov,4)<0){
            perror("error en writev");
            close(s);
            return -1;
        }
        // recivo longitud
        if(recv(s, &longitud, sizeof(int), MSG_WAITALL) != sizeof(int)){
            perror("error en recv");
            close(s);
            return -1;
        }
        
        longitud = ntohl(longitud);
        
        if(longitud==0){
            map_iter_next(it);
            continue;
        }
            
        // recivo mensaje
        char *mensaje = malloc(longitud);
        if(recv(s, mensaje, longitud, MSG_WAITALL) != longitud){
            perror("error en recv");
            close(s);
            return -1;
        }
        printf("STILL ALIVE\n");

        *topic = strdup(e->tema);
        *msg = mensaje;
        e->offset++;

        map_iter_next(it);
        pos = map_iter_exit(it);

        return longitud;
    }

    return 0;
    
}

// QUINTA FASE: COMMIT OFFSETS

// Cliente guarda el offset especificado para ese tema.
// Devuelve 0 si OK y un número negativo en caso de error.
int commit(char *client, char *topic, int offset) {
    struct iovec iov[6];
    
    // envio codigo de operacion
    int nelem = 0;
    int entero_net = htonl(6);
    iov[nelem].iov_base=&entero_net;
    iov[nelem++].iov_len=sizeof(int);

    // envio nombre de topic
    int longitud_topic = strlen(topic);
    int longitud_topic_net = htonl(longitud_topic);
    iov[nelem].iov_base=&longitud_topic_net;
    iov[nelem++].iov_len=sizeof(int);
    iov[nelem].iov_base=topic;
    iov[nelem++].iov_len=longitud_topic;
    printf("envio topic %s\n", topic);

    // envio nombre de cliente
    int longitud_cl = strlen(client);
    int longitud_cl_net = htonl(longitud_cl);
    iov[nelem].iov_base=&longitud_cl_net;
    iov[nelem++].iov_len=sizeof(int);
    iov[nelem].iov_base=client;
    iov[nelem++].iov_len=longitud_cl;
    printf("envio cliente %s\n", client);

    // envio offset
    int offset_net = htonl(offset);
    iov[nelem].iov_base=&offset_net;
    iov[nelem++].iov_len=sizeof(int);
    printf("envio offset %d\n", offset);

    if(writev(s,iov,6)<0){
        perror("error en writev");
        close(s);
        return -1;
    }

    int res;

    if(recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int)){
        perror("error en recv");
        close(s);
        return -1;
    }
    
    return ntohl(res);
}

// Cliente obtiene el offset guardado para ese tema.
// Devuelve el offset y un número negativo en caso de error.
int commited(char *client, char *topic) {
    
    struct iovec iov[5];

    // envio codigo de operacion
    int nelem = 0;
    int entero_net = htonl(7);
    iov[nelem].iov_base=&entero_net;
    iov[nelem++].iov_len=sizeof(int);

    // envio nombre de topic
    int longitud_topic = strlen(topic);
    int longitud_topic_net = htonl(longitud_topic);
    iov[nelem].iov_base=&longitud_topic_net;
    iov[nelem++].iov_len=sizeof(int);
    iov[nelem].iov_base=topic;
    iov[nelem++].iov_len=longitud_topic;

    // envio nombre de cliente
    int longitud_cl = strlen(client);
    int longitud_cl_net = htonl(longitud_cl);
    iov[nelem].iov_base=&longitud_cl_net;
    iov[nelem++].iov_len=sizeof(int);
    iov[nelem].iov_base=client;
    iov[nelem++].iov_len=longitud_cl;

    if(writev(s,iov,5)<0){
        perror("error en writev");
        close(s);
        return -1;
    }

    int res;
    if(recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int)){
        perror("error en recv");
        close(s);
        return -1;
    }

    return ntohl(res);
}

