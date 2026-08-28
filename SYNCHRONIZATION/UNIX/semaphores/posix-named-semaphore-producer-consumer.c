#include <stdio.h>
#include <stdlib.h>
#include <sys/sem.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <semaphore.h>
#include <fcntl.h>
#include <unistd.h>

#define SIZE (1000)        // Dimensione del buffer circolare
#define END (10000000)     // Valore finale da raggiungere

#define AUDIT if(0)        // Flag per debug (se 1 stampa i valori letti)

long *v; // Puntatore alla memoria condivisa

// Array di semafori: uno per ogni slot del buffer
sem_t *sem_free_slot[SIZE], *sem_available_item[SIZE];

void * producer(void){
    long data = 0;
    long my_index = 0;

    printf("ready to produce\n");

retry:
    // Attende che lo slot corrente sia libero (valore del semaforo > 0)
    sem_wait(sem_free_slot[my_index]);
    
    // Scrive il dato nella memoria condivisa
    v[my_index] = data;

    // Segnala che lo slot ora contiene un nuovo elemento pronto per il consumatore
    sem_post(sem_available_item[my_index]);

    // Sposta l'indice al prossimo slot (gestione circolare)
    my_index = (my_index + 1) % SIZE;
    data++;

    goto retry;
}

void * consumer(void){
    long data = 0;
    long my_index = 0;
    long value;

    printf("ready to consume\n");

retry:
    // Attende che il produttore abbia scritto qualcosa nello slot corrente
    sem_wait(sem_available_item[my_index]);
    
    // Legge il valore dalla memoria condivisa
    value = v[my_index];

    AUDIT
    printf("consumer got value %ld\n", value);

    // Verifica di coerenza: il dato deve essere sequenziale
    if(value != data){
        printf("consumer: synch protocol broken at expected value: %ld - real is %ld!!\n", data + 1, value);
        exit(-1);
    }

    // Condizione di uscita
    if (value == END){
        printf("ending condition met - last read value is %ld\n", value);
        exit(0);
    }

    // Segnala che lo slot è stato letto ed è ora libero per il produttore
    sem_post(sem_free_slot[my_index]);

    // Sposta l'indice al prossimo slot
    my_index = (my_index + 1) % SIZE;
    data++;

    goto retry;
}

int main(int argc, char** argv){
    int prod, cons;
    int i;
    char buff[128];

    // Creazione dei semafori per i posti liberi (inizializzati a 1)
    for (i = 0; i < SIZE; i++){
        sprintf(buff, "slot%d", i);
        sem_free_slot[i] = sem_open(buff, O_CREAT, 0666, 1);
        if (sem_free_slot[i] == SEM_FAILED){
            perror("sem_open free_slot");
            exit(1);
        }
        sem_unlink(buff); // Rimuove il nome dal sistema, ma il semaforo resta attivo finché aperto
    }

    // Creazione dei semafori per gli elementi disponibili (inizializzati a 0)
    for (i = 0; i < SIZE; i++){
        sprintf(buff, "item%d", i);
        sem_available_item[i] = sem_open(buff, O_CREAT, 0666, 0);
        if (sem_available_item[i] == SEM_FAILED){
            perror("sem_open available_item");
            exit(1);
        }
        sem_unlink(buff);
    }

    // Allocazione della memoria condivisa tra i processi
    v = (long*)mmap(NULL, SIZE * sizeof(long), PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_SHARED, -1, 0);
    if (v == MAP_FAILED){
        perror("mmap");
        exit(-1);
    }
    
    // Fork per creare il processo Produttore
    prod = fork();
    if (prod == 0){
        producer(); // Codice eseguito solo dal figlio
    }

    // Fork per creare il processo Consumatore
    cons = fork();
    if (cons == 0){
        consumer(); // Codice eseguito solo dal secondo figlio
    }

    // Il padre attende la terminazione dei figli
    wait(NULL);
    wait(NULL);
    
    exit(0);
}
