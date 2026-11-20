#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <semaphore.h>
#include <time.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/msg.h>

#define FIFO_NAME "/tmp/hospital_fifo"   // іменований канал між медсестрами
#define SEM_NAME  "/hospital_rest"       // семафор "вихід з кабінету"
#define LOG_FILE  "breaks.log"           // журнал перерв

typedef struct { int id; int critical; } Patient;          // дані, що йдуть у FIFO
typedef struct { long mtype; int id; int score; } Result;  // повідомлення в черзі
static sem_t *rest;

// Поточний час у форматі ГГ:ХХ:СС.мс
static void now(char *buf) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    strftime(buf, 16, "%H:%M:%S", localtime(&tv.tv_sec));
    sprintf(buf + 8, ".%03ld", tv.tv_usec / 1000);
}

// d) Перерва: семафор зі значенням 1 випускає з кабінету лише одного працівника
static void maybe_break(const char *who) {
    if (rand() % 100 >= 30) return;          // перерва трапляється у 30% випадків
    char start[16], end[16];
    sem_wait(rest);
    now(start);
    printf("   [перерва] %s: вихід о %s\n", who, start);
    usleep(200000 + rand() % 300000);        // відпочинок 0.2-0.5 с
    now(end);
    printf("   [перерва] %s: повернення о %s\n", who, end);
    FILE *log = fopen(LOG_FILE, "a");
    fprintf(log, "вихід: %s | повернення: %s | %s\n", start, end, who);
    fclose(log);
    sem_post(rest);
}

// Перша медсестра: читає номери з pipe, перевіряє показники, передає у FIFO
static void nurse1(int pfd[2]) {
    close(pfd[1]);
    kill(getppid(), SIGUSR1);                // a) сигнал "готова до роботи"
    int fifo = open(FIFO_NAME, O_WRONLY);
    Patient p;
    while (read(pfd[0], &p.id, sizeof(int)) > 0) {
        p.critical = rand() % 100 < 20;      // b) у 20% випадків - критичні показники
        printf("Медсестра 1: пацієнт #%d, показники %s\n", p.id, p.critical ? "КРИТИЧНІ" : "в нормі");
        write(fifo, &p, sizeof(p));
        maybe_break("Медсестра 1");
    }
    close(fifo);
}

// Друга медсестра: читає FIFO, тестує стабільних, пише результат у чергу повідомлень
static void nurse2(int msq) {
    kill(getppid(), SIGUSR2);                // a) сигнал "готова до роботи"
    int fifo = open(FIFO_NAME, O_RDONLY);
    Patient p;
    while (read(fifo, &p, sizeof(p)) > 0) {
        printf("Медсестра 2: пацієнт #%d - %s\n", p.id, p.critical ? "критичний стан" : "стабільний стан");
        if (!p.critical) {                   // c) експрес-тест, бал від 1 до 6
            Result r = {1, p.id, rand() % 6 + 1};
            msgsnd(msq, &r, sizeof(r) - sizeof(long), 0);
        }
        maybe_break("Медсестра 2");
    }
    Result end = {1, -1, 0};                 // маркер "пацієнтів більше немає"
    msgsnd(msq, &end, sizeof(end) - sizeof(long), 0);
    close(fifo);
}

int main(int argc, char *argv[]) {
    if (argc != 2) { fprintf(stderr, "Використання: %s <кількість пацієнтів>\n", argv[0]); return 1; }
    setbuf(stdout, NULL);                    // вивід без буфера, щоб рядки не дублювались після fork

    int pfd[2];
    pipe(pfd);                                          // неіменований канал
    unlink(FIFO_NAME);
    mkfifo(FIFO_NAME, 0600);                            // іменований канал
    int msq = msgget(IPC_PRIVATE, IPC_CREAT | 0600);    // черга повідомлень
    sem_unlink(SEM_NAME);
    rest = sem_open(SEM_NAME, O_CREAT, 0600, 1);        // семафор
    fclose(fopen(LOG_FILE, "w"));                       // очищаємо журнал перерв

    // Блокуємо SIGUSR1/SIGUSR2 до fork, щоб сигнал не загубився; приймаємо їх через sigwait
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGUSR1);
    sigaddset(&set, SIGUSR2);
    sigprocmask(SIG_BLOCK, &set, NULL);

    if (fork() == 0) { srand(getpid()); nurse1(pfd); exit(0); }
    if (fork() == 0) { srand(getpid()); close(pfd[0]); close(pfd[1]); nurse2(msq); exit(0); }
    srand(getpid());
    close(pfd[0]);

    for (int i = 0, sig; i < 2; i++) {       // a) чекаємо сигнали від обох медсестер
        sigwait(&set, &sig);
        printf("Завідувач: медсестра %d готова до роботи\n", sig == SIGUSR1 ? 1 : 2);
    }

    int n = atoi(argv[1]);                   // a) кількість пацієнтів з аргументів
    printf("Завідувач: кількість пацієнтів - %d\n", n);
    for (int i = 0; i < n; i++) {
        int id = 1000 + rand() % 9000;
        printf("Завідувач: пацієнт #%d -> pipe\n", id);
        write(pfd[1], &id, sizeof(id));
    }
    close(pfd[1]);                           // медсестра 1 отримає EOF

    Result r;                                // c) читаємо результати з черги повідомлень
    while (msgrcv(msq, &r, sizeof(r) - sizeof(long), 0, 0) > 0 && r.id != -1) {
        printf("Завідувач: пацієнт #%d, бал за тестом - %d\n", r.id, r.score);
        maybe_break("Завідувач");
    }

    while (wait(NULL) > 0);                  // чекаємо завершення медсестер
    msgctl(msq, IPC_RMID, NULL);
    unlink(FIFO_NAME);
    sem_close(rest);
    sem_unlink(SEM_NAME);
    printf("Завідувач: зміну завершено, журнал перерв - %s\n", LOG_FILE);
    return 0;
}
