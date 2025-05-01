#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

#define MAX_QUEUE_SIZE 100
#define LINE_SIZE 256

typedef struct
{
    char flightID[20];
    int priority;
    int departureTime;
    int landingTime;
    char route[100];
} Flight;

// Queue struct
typedef struct
{
    Flight *flights[MAX_QUEUE_SIZE];
    int front, rear, size;
    pthread_mutex_t mutex;
    pthread_cond_t notEmpty;
    pthread_cond_t notFull;
    int shutdown;
} FlightQueue;

// Initialize queue
void initQueue(FlightQueue *q)
{
    q->front = 0;
    q->rear = 0;
    q->size = 0;
    q->shutdown = 0;
    pthread_mutex_init(&q->mutex, NULL);
    pthread_cond_init(&q->notEmpty, NULL);
    pthread_cond_init(&q->notFull, NULL);
}

// Enqueue
void enqueue(FlightQueue *q, Flight *f)
{
    pthread_mutex_lock(&q->mutex);
    while (q->size == MAX_QUEUE_SIZE && !q->shutdown)
    {
        pthread_cond_wait(&q->notFull, &q->mutex);
        printf("\033[32mProducer Function Waiting \033[0m\n");
    }

    if (q->shutdown)
    {
        pthread_mutex_unlock(&q->mutex);
        return;
    }

    q->flights[q->rear] = f;
    q->rear = (q->rear + 1) % MAX_QUEUE_SIZE;
    q->size++;
    printf("\033[32mProducer Function Writing to Shared Memory \033[0m\n");
    pthread_cond_signal(&q->notEmpty);
    pthread_mutex_unlock(&q->mutex);
}

// Dequeue
Flight *dequeue(FlightQueue *q)
{
    // printf("\033[34mConsumer Function checking Shared Memory \033[0m\n");
    pthread_mutex_lock(&q->mutex);
    while (q->size == 0 && !q->shutdown)
    {
        pthread_cond_wait(&q->notEmpty, &q->mutex);
        printf("\033[34mConsumer Function\033[31m Waiting \033[0m\033[0m\n");
    }

    if (q->shutdown && q->size == 0)
    {
        pthread_mutex_unlock(&q->mutex);
        printf("\033[34mShared Memory\033[31m EMPTY \033[0m  \033[0m\n");
        return NULL;
    }

    Flight *f = q->flights[q->front];
    q->front = (q->front + 1) % MAX_QUEUE_SIZE;
    q->size--;
    printf("\033[34mConsumer Function Reading Shared Memory\033[0m\n");
    pthread_cond_signal(&q->notFull);
    pthread_mutex_unlock(&q->mutex);
    return f;
}

// Shutdown signal
void shutdownQueue(FlightQueue *q)
{
    pthread_mutex_lock(&q->mutex);
    q->shutdown = 1;
    pthread_cond_broadcast(&q->notEmpty);
    pthread_cond_broadcast(&q->notFull);
    pthread_mutex_unlock(&q->mutex);
}

// Global queues
FlightQueue takeoffQueue;
FlightQueue landingQueue;

// Utility: CSV parser
Flight *parseFlight(char *line)
{
    Flight *f = malloc(sizeof(Flight));
    if (!f)
        return NULL;

    char *token = strtok(line, ",");
    if (!token)
        return NULL;
    strncpy(f->flightID, token, sizeof(f->flightID));

    token = strtok(NULL, ",");
    f->priority = token ? atoi(token) : 0;

    token = strtok(NULL, ",");
    f->departureTime = token ? atoi(token) : 0;

    token = strtok(NULL, ",");
    f->landingTime = token ? atoi(token) : 0;

    token = strtok(NULL, ",");
    strncpy(f->route, token ? token : "", sizeof(f->route));

    return f;
}

// Producer: reads CSV and populates queues
void *producer(void *arg)
{
    FILE *file = fopen("flights.csv", "r");
    if (!file)
    {
        perror("Error opening flights.csv");
        return NULL;
    }

    char line[LINE_SIZE];
    while (fgets(line, sizeof(line), file))
    {
        Flight *f = parseFlight(line);
        if (!f)
            continue;

        if (f->departureTime > f->landingTime)
            enqueue(&takeoffQueue, f);
        else
            enqueue(&landingQueue, f);
    }

    fclose(file);

    shutdownQueue(&takeoffQueue);
    shutdownQueue(&landingQueue);
    return NULL;
}

// Consumer: takeoff processor
void *takeoffConsumer(void *arg)
{
    while (1)
    {
        Flight *f = dequeue(&takeoffQueue);
        if (!f)
            break;

        printf("\n\033[33m---->  Taking off flight %s (Priority: %d)\033[0m\n", f->flightID, f->priority);
        sleep(f->departureTime);
        free(f);
    }
    printf("\033[33m---->  Takeoff consumer exiting.\033[0m\n");
    return NULL;
}

// Consumer: landing processor
void *landingConsumer(void *arg)
{
    while (1)
    {
        Flight *f = dequeue(&landingQueue);
        if (!f)
            break;

        printf("\n\033[33m---->  Landing flight %s (Priority: %d)\033[0m\n", f->flightID, f->priority);
        usleep(f->landingTime * 1000); // in milliseconds
        free(f);
    }
    printf("\033[33m---->  Landing consumer exiting.\033[0m\n");
    return NULL;
}
// printf("\033[31m  \033[0m");
int main()
{
    system("clear");
    // printf("\n\n\033[31m Queueing Flights W.r.t there Priority \033[0m\n");
    initQueue(&takeoffQueue);
    initQueue(&landingQueue);

    pthread_t prodThread, takeoffThread, landingThread;

    pthread_create(&prodThread, NULL, producer, NULL);
    pthread_create(&takeoffThread, NULL, takeoffConsumer, NULL);
    pthread_create(&landingThread, NULL, landingConsumer, NULL);

    pthread_join(prodThread, NULL);
    pthread_join(takeoffThread, NULL);
    pthread_join(landingThread, NULL);

    printf("\n All processes completed.\n");
    return 0;
}
