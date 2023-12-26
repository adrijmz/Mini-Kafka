#include <stdio.h>
#include <string.h>
#include "comun.h"
#include "map.h"
#include "queue.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <pthread.h>

// información que se la pasa el thread creado
typedef struct thread_info {
    int socket; // añadir los campos necesarios
} thread_info;

typedef struct my_entry{
    char *tema;
    int offset;
    queue *cola;
} my_entry;

typedef struct msj{
    int longitud;
    char *mensaje;
} msj;

struct map *mapa;
char *dir_commited;

void *servicio(void *arg){
    int entero;
    int longitud;
    char *string;
    thread_info *thinf = arg; // argumento recibido
    int err = 0;

    // si recv devuelve <=0 el cliente ha cortado la conexión;
    // recv puede devolver menos datos de los solicitados
    // (misma semántica que el "pipe"), pero con MSG_WAITALL espera hasta que
    // se hayan recibido todos los datos solicitados o haya habido un error.
    while (1) {
        // cada "petición" comienza con un entero
        if (recv(thinf->socket, &entero, sizeof(int), MSG_WAITALL)!=sizeof(int))
            break;
        entero = ntohl(entero);
        printf("Recibido entero: %d\n", entero);

        //se crea un nuevo tema
        if(entero==0){
            // luego llega el string, que viene precedido por su longitud
            if (recv(thinf->socket, &longitud, sizeof(int), MSG_WAITALL)!=sizeof(int))
                break;
            longitud = ntohl(longitud);
            string = malloc(longitud+1); // +1 para el carácter nulo
            // ahora sí llega el string
            if (recv(thinf->socket, string, longitud, MSG_WAITALL)!=longitud)
                break;
            string[longitud]='\0';       // añadimos el carácter nulo
            printf("Recibido topic: %s\n", string); 

            // crea un cola vacia
            struct queue *q = queue_create(0);
            // crea una entry
            my_entry *e = malloc(sizeof(my_entry));
            e->tema = string;
            e->cola = q;
            e->offset = 0;

            //añade la entry al mapa
            int cod = map_put(mapa, string, e);
            
            // envía un entero como respuesta
            int res = htonl(cod);
            write(thinf->socket, &res, sizeof(int)); 
        }

        // envio tamaño del mapa
        else if(entero==1){
            int tam = map_size(mapa);
            int res = htonl(tam);
            write(thinf->socket, &res, sizeof(int)); 
        }

        // creo un nuevo mensaje
        else if(entero==2){
            // luego llega el string, que viene precedido por su longitud
            if (recv(thinf->socket, &longitud, sizeof(int), MSG_WAITALL)!=sizeof(int))
                break;
            longitud = ntohl(longitud);
            char *topic = malloc(longitud+1); // +1 para el carácter nulo
            // ahora sí llega el string
            if (recv(thinf->socket, topic, longitud, MSG_WAITALL)!=longitud)
                break;
            topic[longitud]='\0';       // añadimos el carácter nulo
            printf("Recibido topic: %s\n", topic); 

            // luego llega el mensaje, que viene precedido por su longitud
            if (recv(thinf->socket, &longitud, sizeof(int), MSG_WAITALL)!=sizeof(int))
                break;
            longitud = ntohl(longitud);
            char *mensaje = malloc(longitud); // +1 para el carácter nulo
            // ahora sí llega el string
            if (recv(thinf->socket, mensaje, longitud, MSG_WAITALL)!=longitud)
                break;
            // mensaje[longitud]='\0';       // añadimos el carácter nulo
            printf("Recibido mensaje: %s con longitud %d\n", mensaje,longitud); 

            // busca la entry en el mapa
            my_entry *e = map_get(mapa, topic, &err);

            if(err==-1){
                int res = htonl(-1);
                printf("No existe el topic\n");
                write(thinf->socket, &res, sizeof(int));
            }
            else{
                
                msj *m = malloc(sizeof(msj));
                m->longitud = longitud;
                m->mensaje = mensaje;

                // añade la operacion a la cola
                queue_append(e->cola, m);

                // devuelve su offset
                int offset = e->offset;
                int res = htonl(offset);
                write(thinf->socket, &res, sizeof(int));

                // actualiza el offset
                e->offset = offset+1;
            }
            
        }

        // envio longitud del mensaje
        else if(entero==3){

            // llega el topic, que viene precedido por su longitud
            if (recv(thinf->socket, &longitud, sizeof(int), MSG_WAITALL)!=sizeof(int))
                break;
            longitud = ntohl(longitud);
            char *topic = malloc(longitud+1); // +1 para el carácter nulo

            // ahora sí llega el topic
            if (recv(thinf->socket, topic, longitud, MSG_WAITALL)!=longitud)
                break;
            topic[longitud]='\0';       // añadimos el carácter nulo
            printf("Recibido topic: %s\n", topic);
            
            // llega el offset
            int offset;
            if (recv(thinf->socket, &offset, sizeof(int), MSG_WAITALL)!=sizeof(int))
                break;
            offset = ntohl(offset);
            printf("Recibido offset: %d\n", offset);

            // busca la entry en el mapa
            my_entry *e = map_get(mapa, topic, &err);      

            if(err==-1){
                int res = htonl(-1);
                printf("No existe el topic\n");
                write(thinf->socket, &res, sizeof(int));
            }
            else{
                // obtiene mensaje de la cola en la posicion offset
                if(offset>=e->offset){
                    int res = htonl(0);
                    printf("No existe el offset\n");
                    write(thinf->socket, &res, sizeof(int));
                }
                else{
                    msj *m = queue_get(e->cola, offset, &err);
                    //envia la longitud del mensaje
                    longitud = m->longitud;
                    int longitud_net = htonl(longitud);
                    write(thinf->socket, &longitud_net, sizeof(int));
                }
            }

        }
        // envio tamaño de la cola
        else if(entero==4){
            if (recv(thinf->socket, &longitud, sizeof(int), MSG_WAITALL)!=sizeof(int))
                break;
            longitud = ntohl(longitud);
            string = malloc(longitud+1); // +1 para el carácter nulo
            // ahora sí llega el string
            if (recv(thinf->socket, string, longitud, MSG_WAITALL)!=longitud)
                break;
            string[longitud]='\0';       // añadimos el carácter nulo
            printf("Recibido topic: %s\n", string);

            // devuelve el tamaño de la cola asociada al topic
            my_entry *e = map_get(mapa, string, &err);
            int res = -1;
            if(err!=-1){
                int tam = queue_size(e->cola);
                res = htonl(tam);
            }
            write(thinf->socket, &res, sizeof(int)); 
            
        }
        else if(entero==5){
            // luego llega el string, que viene precedido por su longitud
            if (recv(thinf->socket, &longitud, sizeof(int), MSG_WAITALL)!=sizeof(int))
                break;
            longitud = ntohl(longitud);
            string = malloc(longitud+1); // +1 para el carácter nulo
            // ahora sí llega el topic
            if (recv(thinf->socket, string, longitud, MSG_WAITALL)!=longitud)
                break;
            string[longitud]='\0';       // añadimos el carácter nulo
            printf("Recibido topic: %s\n", string); 

            //llega el offset
            int offset;
            if (recv(thinf->socket, &offset, sizeof(int), MSG_WAITALL)!=sizeof(int))
                break;
            offset = ntohl(offset);
            printf("Recibido offset: %d\n", offset);
            

            // busca la entry en el mapa
            my_entry *e = map_get(mapa, string, &err);

            if(err==-1){
                int res = htonl(-1);
                printf("No existe el topic\n");
                write(thinf->socket, &res, sizeof(int));
            }
            struct iovec iov[2];

            // si no hay mensajes nuevos manda 0
            if(offset>=e->offset){
                printf("No hay mensajes nuevos\n");
                
                int longitud_net = htonl(0);
                iov[0].iov_base = &longitud_net;
                iov[0].iov_len = sizeof(int);
                writev(thinf->socket, iov, 1);
            }
            else{
                // devuelve el siguiente mensaje sin leer
                msj *m = queue_get(e->cola, offset, &err);
                //envia el mensaje
                longitud = m->longitud;
                int longitud_net = htonl(longitud);
                iov[0].iov_base = &longitud_net;
                iov[0].iov_len = sizeof(int);
                iov[1].iov_base = m->mensaje;
                iov[1].iov_len = longitud;
                writev(thinf->socket, iov, 2);

            }
        }
        else if(entero==6){
            // recivo nombre de topic
            if (recv(thinf->socket, &longitud, sizeof(int), MSG_WAITALL)!=sizeof(int))
                break;
            longitud = ntohl(longitud);
            printf("longitud: %d\n", longitud);
            string = malloc(longitud+1); // +1 para el carácter nulo
            // ahora sí llega el topic
            if (recv(thinf->socket, string, longitud, MSG_WAITALL)!=longitud)
                break;
            printf("STILL HERE\n");
            string[longitud]='\0';       // añadimos el carácter nulo
            printf("Recibido topic: %s\n", string);

            // recivo nombre de cliente
            if (recv(thinf->socket, &longitud, sizeof(int), MSG_WAITALL)!=sizeof(int))
                break;
            longitud = ntohl(longitud);
            printf("longitud: %d\n", longitud);
            char *cliente = malloc(longitud+1); // +1 para el carácter nulo
            // ahora sí llega el topic
            if (recv(thinf->socket, cliente, longitud, MSG_WAITALL)!=longitud)
                break;
            cliente[longitud]='\0';       // añadimos el carácter nulo
            printf("Recibido cliente: %s\n", cliente);


            // llega el offset
            int offset;
            if (recv(thinf->socket, &offset, sizeof(int), MSG_WAITALL)!=sizeof(int))
                break;
            offset = ntohl(offset);
            printf("Recibido offset: %d\n", offset);

            //comprueba si el topic existe
            my_entry *e = map_get(mapa, string, &err);
            if(e==NULL){
                int res = htonl(-1);
                printf("No existe el topic\n");
                write(thinf->socket, &res, sizeof(int));
            }
            else{

                // crea un subidrectorio con el nombre del cliente en el directorio dir_commited
                char *dir = malloc(strlen(dir_commited)+strlen(cliente)+2);
                strcpy(dir,dir_commited);
                strcat(dir,"/");
                strcat(dir,cliente);
                mkdir(dir,0777);

                // guarda el offset en un fichero con el nombre del topic en el subdirectorio del cliente

                char *fichero = malloc(strlen(dir)+strlen(string)+2);
                strcpy(fichero,dir);
                strcat(fichero,"/");
                strcat(fichero,string);

                FILE *f = fopen(fichero,"w");
                if(f==NULL){
                    printf("Error al abrir el fichero\n");
                    return -1;
                }
                fprintf(f,"%d",offset);
                fclose(f);

                int res = htonl(0);
                write(thinf->socket, &res, sizeof(int));
            }
        }
        else if(entero==7){
            // recivo nombre de topic
            if (recv(thinf->socket, &longitud, sizeof(int), MSG_WAITALL)!=sizeof(int))
                break;
            longitud = ntohl(longitud);
            printf("longitud: %d\n", longitud);
            string = malloc(longitud+1); // +1 para el carácter nulo
            // ahora sí llega el topic
            if (recv(thinf->socket, string, longitud, MSG_WAITALL)!=longitud)
                break;
            printf("STILL HERE\n");
            string[longitud]='\0';       // añadimos el carácter nulo
            printf("Recibido topic: %s\n", string);

            // recivo nombre de cliente
            if (recv(thinf->socket, &longitud, sizeof(int), MSG_WAITALL)!=sizeof(int))
                break;
            longitud = ntohl(longitud);
            printf("longitud: %d\n", longitud);
            char *cliente = malloc(longitud+1); // +1 para el carácter nulo
            // ahora sí llega el topic
            if (recv(thinf->socket, cliente, longitud, MSG_WAITALL)!=longitud)
                break;
            cliente[longitud]='\0';       // añadimos el carácter nulo
            printf("Recibido cliente: %s\n", cliente);

            // compruebo si existe el topic
            my_entry *e = map_get(mapa, string, &err);
            if(e==NULL){
                int res = htonl(-1);
                printf("No existe el topic\n");
                write(thinf->socket, &res, sizeof(int));
            }
            else{
                // compruebo si existe el cliente
                char *dir = malloc(strlen(dir_commited)+strlen(cliente)+2);
                strcpy(dir,dir_commited);
                strcat(dir,"/");
                strcat(dir,cliente);
                if(access(dir,F_OK)==-1){
                    int res = htonl(-1);
                    printf("No existe el cliente\n");
                    write(thinf->socket, &res, sizeof(int));
                }
                else{     
                    // compruebo si existe el fichero
                    char *fichero = malloc(strlen(dir)+strlen(string)+2);
                    strcpy(fichero,dir);
                    strcat(fichero,"/");
                    strcat(fichero,string);

                    FILE *f = fopen(fichero,"r");
                    if(f==NULL){
                        printf("Error al abrir el fichero\n");
                        int res = htonl(-1);
                        printf("No existe el cliente\n");
                        write(thinf->socket, &res, sizeof(int));
                    }
                    else{
                        int offset;
                        fscanf(f,"%d",&offset);
                        fclose(f);

                        // devuelve el offset
                        int res = htonl(offset);
                        write(thinf->socket, &res, sizeof(int));
                    }
                }

            }
        }
    }

    close(thinf->socket);
    return NULL;
}

// inicializa el socket y lo prepara para aceptar conexiones
static int init_socket_server(const char * port) {
    int s;
    struct sockaddr_in dir;
    int opcion=1;
    // socket stream para Internet: TCP
    if ((s=socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
        perror("error creando socket");
        return -1;
    }
    // Para reutilizar puerto inmediatamente si se rearranca el servidor
    if (setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &opcion, sizeof(opcion))<0){
        perror("error en setsockopt");
        return -1;
    }
    // asocia el socket al puerto especificado
    dir.sin_addr.s_addr=INADDR_ANY;
    dir.sin_port=htons(atoi(port));
    dir.sin_family=PF_INET;
    if (bind(s, (struct sockaddr *)&dir, sizeof(dir)) < 0) {
        perror("error en bind");
        close(s);
        return -1;
    }
    // establece el nº máx. de conexiones pendientes de aceptar
    if (listen(s, 5) < 0) {
        perror("error en listen");
        close(s);
        return -1;
    }
    return s;
}

int main(int argc, char *argv[]) {

    int s, s_conec;
    unsigned int tam_dir;
    struct sockaddr_in dir_cliente;

    if (argc!=2 && argc!=3) {
        fprintf(stderr, "Uso: %s puerto [dir_commited]\n", argv[0]);
        return 1;
    }

    // inicializa el socket y lo prepara para aceptar conexiones
    if ((s=init_socket_server(argv[1])) < 0) return -1;

    // prepara atributos adecuados para crear thread "detached"
    pthread_t thid;
    pthread_attr_t atrib_th;
    pthread_attr_init(&atrib_th); // evita pthread_join
    pthread_attr_setdetachstate(&atrib_th, PTHREAD_CREATE_DETACHED);

    //crea el mapa
    mapa = map_create(key_string,0);

    // copia en dir_commited la ruta al directorio de los mensajes confirmados
    if (argc==3) {
        dir_commited = malloc(strlen(argv[2])+1);
        strcpy(dir_commited, argv[2]);
    }

    while(1) {
        tam_dir=sizeof(dir_cliente);
        // acepta la conexión
        if ((s_conec=accept(s, (struct sockaddr *)&dir_cliente, &tam_dir))<0){
            perror("error en accept");
            close(s);
            return -1;
        }
        // crea el thread de servicio
        thread_info *thinf = malloc(sizeof(thread_info));
        thinf->socket=s_conec;
        pthread_create(&thid, &atrib_th, servicio, thinf);
    }
    close(s); // cierra el socket general

    return 0;
}
