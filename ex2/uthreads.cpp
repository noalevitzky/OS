#include "uthreads.h"
#include <iostream>
#include <set>
#include <map>
#include <deque>
#include <stdio.h>
#include <setjmp.h>
#include <signal.h>
#include <unistd.h>
#include <sys/time.h>
#include <algorithm>

//blackbox translation
#ifdef __x86_64__
/* code for 64 bit Intel arch */

typedef unsigned long address_t;
#define JB_SP 6
#define JB_PC 7

/* A translation is required when using an address of a variable.
   Use this as a black box in your code. */
address_t translate_address(address_t addr)
{
    address_t ret;
    asm volatile("xor    %%fs:0x30,%0\n"
		"rol    $0x11,%0\n"
                 : "=g" (ret)
                 : "0" (addr));
    return ret;
}

#else
/* code for 32 bit Intel arch */

typedef unsigned int address_t;
#define JB_SP 4
#define JB_PC 5

/* A translation is required when using an address of a variable.
   Use this as a black box in your code. */
address_t translate_address(address_t addr)
{
    address_t ret;
    asm volatile("xor    %%gs:0x18,%0\n"
                 "rol    $0x9,%0\n"
    : "=g" (ret)
    : "0" (addr));
    return ret;
}

#endif


struct thread{

    int id;
    int quantums;
    char stack[STACK_SIZE];
    void (*f)(void);
    sigjmp_buf env;


    /*
     * Constructor for main thread case
     * */
//    thread (): id(0), quantums(0), stack(nullptr), f(NULL)
//    {
//    }
    /*
     * Constructor for thread which is not main
     * */
//    thread(int id, void (*f)(void)): id(id), quantums(0), stack(new char[STACK_SIZE])
//    {
//        //setup env
//        address_t sp, pc;
//        sp = (address_t)stack + STACK_SIZE - sizeof(address_t);
//        pc = (address_t)f;
//        sigsetjmp(env, 1);
//        (env->__jmpbuf)[JB_SP] = translate_address(sp);
//        (env->__jmpbuf)[JB_PC] = translate_address(pc);
//        sigemptyset(&env->__saved_mask);
//    }

//    ~thread(){
//        if (stack != nullptr){
//            delete[](stack);
//        }
//    }
};

// library variables
std::map<int, thread*> threads;
int running;
std::deque<int> ready;               //holds threads in ready queue
std::deque<int> blocked;             //holds blocked threads
std::deque<int> mutex_blocked;       //holds mutex blocked threads
std::deque<int> double_blocked;      //holds mutex blocked threads
std::set<int> free_ids;              //holds free threads ids
bool lock;
int lock_thread;
int total_quantums = 0;
int time_q;
struct itimerval timer;
struct sigaction sa = {};
sigset_t set;
void round_robin();

void print_library_error(std::string error)
{
    std::cerr << "thread library error: " << error << std::endl;
}

void print_system_error(std::string error)
{
    std::cerr << "system error: " << error << std::endl;
}

/*
 * get min avail tid and remove it from free_ids
 */
int get_minimum_free_id(){
    int tid = *free_ids.begin();
    free_ids.erase(tid);
    return tid;
}

void free_library_memory(){
    auto it = threads.begin();
    while (it != threads.end()){
        if (it->first != 0){
            free(it->second);
        }
        it++;
    }
    threads.clear();
}


void init_set_for_alarm(){
    // define signal set for alarm
    if(sigemptyset(&set))
    {
        print_system_error("sigemptyset failed");
        free_library_memory();
        exit(1);
    }
    if (sigaddset(&set, SIGVTALRM))
    {
        print_system_error("sigaddset failed");
        free_library_memory();
        exit(1);
    }
}

void block_alarm()
{
    init_set_for_alarm();
    if (sigprocmask(SIG_BLOCK, &set, NULL)){ // mask fails
        print_system_error("unable to block alarm");
        free_library_memory();
        exit(1);
    }
}

void unblock_alarm()
{

    init_set_for_alarm();
    if (sigprocmask(SIG_UNBLOCK, &set, NULL))
    {
        print_system_error("unable to unblock alarm");
        free_library_memory();
        exit(1);
    }
}

/**
 * lock mutex and set lock tid
 */
void lockMutex()
{
    lock = true;
    lock_thread = uthread_get_tid();
}

/**
 * unlock mutex and reset lock tid
 */
void unlockMutex()
{
    lock = false;
    lock_thread = -1;
}

bool is_mutex_locked() {
    return lock;
}


void move_current_to_ready(){
    int val = sigsetjmp(threads[uthread_get_tid()]->env,1);
    if (val == 1){
        return;
    }
    block_alarm();
    ready.push_back(uthread_get_tid());
    running = -1;
}

void move_current_to_blocked(){
    blocked.push_back(uthread_get_tid());
    int val = sigsetjmp(threads[uthread_get_tid()]->env,1);
    if (val == 1){
        return;
    }
    //running = -1;

}

void move_current_to_mutex_blocked(){

    mutex_blocked.push_back(uthread_get_tid());

    //running = -1;

}

/*
 * used in order to ignore timer
 */
//void dummy_handler(int i){
//    return;
//}


void quantum_expires(int sig)
{
    move_current_to_ready(); // save context and add id to ready list
    round_robin(); // find next ready thread, remove from ready list and update running

}

void config_timer(){

    // Configure the timer to expire after quantums sec... */
    timer.it_value.tv_sec = 0;		// first time interval, seconds part
    timer.it_value.tv_usec = time_q;		// first time interval, microseconds part
    timer.it_interval.tv_sec = 0;		// first time interval, seconds part
    timer.it_interval.tv_usec = 0;		// first time interval, microseconds part
    if (setitimer (ITIMER_VIRTUAL, &timer, NULL))
    {
        print_system_error("unable to set timer");
        free_library_memory();
        exit(1);
    }

}


void init_timer(){
    // Install timer_handler as the signal handler for SIGVTALRM.
    sa.sa_handler = &quantum_expires;
    if (sigaction(SIGVTALRM, &sa, NULL) < 0) {
        print_system_error("unable to install timer handler");
        free_library_memory();
        exit(1);
    }
    config_timer();
}

void round_robin(){
    if (ready.empty()){ return;}

    // ready queue is not empty
    int next_thread_id = ready.front();
    ready.pop_front();
    running=next_thread_id;
    total_quantums++;
    threads[next_thread_id]->quantums++;

    //Every time a thread is moved to RUNNING state, it is allocated a quantum of microseconds to run.
    /* Start a virtual timer. It counts down whenever this process is executing. */
    config_timer();
    unblock_alarm();

//    static struct sigaction sa = {nullptr};
//    // Install timer_handler as the signal handler for SIGVTALRM.
//    sa.sa_handler = &quantum_expires;
//    if (sigaction(SIGVTALRM, &sa, NULL) < 0) {
//        print_system_error("unable to install timer handler");
//        free_library_memory();
//        exit(1);
//    }
    siglongjmp(threads[next_thread_id]->env ,1);
}

bool check_if_blocked(int tid)
{
    return std::find(blocked.begin(), blocked.end(), tid) != blocked.end();
}

bool check_if_mutex_blocked(int tid)
{
    return std::find(mutex_blocked.begin(), mutex_blocked.end(), tid) != mutex_blocked.end();
}

bool check_if_double_blocked(int tid)
{
    return std::find(double_blocked.begin(), double_blocked.end(), tid) != double_blocked.end();
}

void remove_from_ready(int tid){
    auto it = std::find(ready.begin(), ready.end(), tid);
    if (it != ready.end()){
        ready.erase(it);
    }
}

void remove_from_blocked(int tid){
    auto it = std::find(blocked.begin(), blocked.end(), tid);
    if (it != blocked.end()){
        blocked.erase(it);
    }
}

void remove_from_mutex_blocked(int tid){
    auto it = std::find(mutex_blocked.begin(), mutex_blocked.end(), tid);
    if (it != mutex_blocked.end()){
        mutex_blocked.erase(it);
    }
}

void remove_from_double_blocked(int tid){
    auto it = std::find(double_blocked.begin(), double_blocked.end(), tid);
    if (it != double_blocked.end()){
        double_blocked.erase(it);
    }
}

/*
 * Description: This function initializes the thread library.
 * You may assume that this function is called before any other thread library
 * function, and that it is called exactly once. The input to the function is
 * the length of a quantum in micro-seconds. It is an error to call this
 * function with non-positive quantum_usecs.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_init(int quantum_usecs){
    if (quantum_usecs <= 0){ // non positive quantum
        print_library_error("quantum usec should be positive number.");
        return -1;
    }
    time_q = quantum_usecs;
    total_quantums = 1;

    // main thread creation and handle
    thread* main_thread = (thread*)malloc(sizeof(thread));

    if (main_thread == nullptr){
        print_system_error("could not allocate main thread");
        exit(1);
    }
    main_thread->id = 0;
    main_thread->quantums = 0;
//    main_thread->stack = NULL;
    main_thread->f = NULL;

    threads[0] = main_thread;
    running = 0;
    threads[0]->quantums = 1;

    //init free ids
    for (int i = 1; i < MAX_THREAD_NUM; i++){
        free_ids.insert(i);
    }

    init_timer();

    return 0;
}

/*
 * Description: This function creates a new thread, whose entry point is the
 * function f with the signature void f(void). The thread is added to the end
 * of the READY threads list. The uthread_spawn function should fail if it
 * would cause the number of concurrent threads to exceed the limit
 * (MAX_THREAD_NUM). Each thread should be allocated with a stack of size
 * STACK_SIZE bytes.
 * Return value: On success, return the ID of the created thread.
 * On failure, return -1.
*/
int uthread_spawn(void (*f)(void)){
    block_alarm();
    if (f == nullptr)
    {
        print_library_error("invalid function as input.");
        unblock_alarm();
        return -1;
    }
    if (threads.size() == MAX_THREAD_NUM){ // limit exceeded
        print_library_error("limit of allowd threads exceeded.");
        unblock_alarm();
        return -1;
    }

    // create thread with min avail tid
    int tid = get_minimum_free_id();
    thread* t = (thread*)malloc(sizeof(thread));
    t->id = tid;
    t-> f = f;
    address_t sp, pc;
    sp = (address_t)t->stack + STACK_SIZE - sizeof(address_t);
    pc = (address_t)f;
    sigsetjmp(t->env, 1);
    (t->env->__jmpbuf)[JB_SP] = translate_address(sp);
    (t->env->__jmpbuf)[JB_PC] = translate_address(pc);
    sigemptyset(&t->env->__saved_mask);

    threads[tid]=t;
    ready.push_back(tid);
    free_ids.erase(tid);

    unblock_alarm();
    return tid;
}


/*
 * Description: This function terminates the thread with ID tid and deletes
 * it from all relevant control structures. All the resources allocated by
 * the library for this thread should be released. If no thread with ID tid
 * exists it is considered an error. Terminating the main thread
 * (tid == 0) will result in the termination of the entire process using
 * exit(0) [after releasing the assigned library memory].
 * Return value: The function returns 0 if the thread was successfully
 * terminated and -1 otherwise. If a thread terminates itself or the main
 * thread is terminated, the function does not return.
*/
int uthread_terminate(int tid){
    block_alarm();
    //tid do not exist
    if (threads.find(tid) == threads.end()) {
        print_library_error("could not terminated thread. Thread id "+ std::to_string(tid) + " doesnt exist");
        unblock_alarm();
        return -1;
    }

    //terminating main thread
    if (tid == 0){
        free_library_memory();
//        unblock_alarm();

        exit(0);
    }

    //terminate running
    if (tid == uthread_get_tid()) {
        block_alarm();

        auto it = threads.find(tid);
        thread* t_terminate = it->second;
        threads.erase(tid);
        free(t_terminate);
        free_ids.insert(tid);
        if (lock && lock_thread==tid) {
            uthread_mutex_unlock();
        }

        round_robin();
        unblock_alarm();
        return 0;
    }

    //terminate non running
    else {
        //free thread from memory
        remove_from_ready(tid);
        remove_from_blocked(tid);
        remove_from_mutex_blocked(tid);
        remove_from_double_blocked(tid);

        auto it = threads.find(tid);
        thread* t_terminate = it->second;
        threads.erase(tid);
        free(t_terminate);
        free_ids.insert(tid);

        unblock_alarm();
        return 0;
    }
}


/*
 * Description: This function blocks the thread with ID tid. The thread may
 * be resumed later using uthread_resume. If no thread with ID tid exists it
 * is considered as an error. In addition, it is an error to try blocking the
 * main thread (tid == 0). If a thread blocks itself, a scheduling decision
 * should be made. Blocking a thread in BLOCKED state has no
 * effect and is not considered an error.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_block(int tid){
    block_alarm();
    //tid do not exist || main was blocked
    if (threads.find(tid) == threads.end()){
        print_library_error("could not block thread. Thread id "+ std::to_string(tid) + " doesnt exist");
        unblock_alarm();
        return -1;
    }
    if (tid == 0) {
        print_library_error("could not block main thread.");
        unblock_alarm();
        return -1;
    }

    // check if running
    if (tid == uthread_get_tid()){
        block_alarm();
        move_current_to_blocked(); // save context, add id to blocked list and remove from running
        round_robin(); // find next ready thread, remove from ready list and update running
        unblock_alarm();
        return 0;
    }

    //thread is blocked already (in blocked or double blocked queue)
    if (check_if_blocked(tid) || check_if_double_blocked(tid)) { return 0;}

    // if already in mutex blocked, move to double blocked
    if (check_if_mutex_blocked(tid)) {
        remove_from_mutex_blocked(tid);
        double_blocked.push_back(tid);
        unblock_alarm();
        return 0;
    }

    //thread is in ready, move to blocked
    remove_from_ready(tid);
    blocked.push_back(tid);
    unblock_alarm();
    return 0;
}


/*
 * Description: This function resumes a blocked thread with ID tid and moves
 * it to the READY state if it"s not synced. Resuming a thread in a RUNNING or READY state
 * has no effect and is not considered as an error. If no thread with
 * ID tid exists it is considered an error.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_resume(int tid){
    block_alarm();
    //tid do not exist
    if (threads.find(tid) == threads.end()) {
        print_library_error("could not resume thread. Thread id "+ std::to_string(tid) + " doesnt exist");
        unblock_alarm();
        return -1;
    }

    //unblock thread
    if (check_if_blocked(tid)) {
        remove_from_blocked(tid);
        ready.push_back(tid);
    }

    //thread is double blocked, move from double to mutex
    else if (check_if_double_blocked(tid)){
        remove_from_double_blocked(tid);
        mutex_blocked.push_back(tid);
    }
    unblock_alarm();
    return 0;
}


/*
 * Description: This function tries to acquire a mutex.
 * If the mutex is unlocked, it locks it and returns.
 * If the mutex is already locked by different thread, the thread moves to BLOCK state.
 * In the future when this thread will be back to RUNNING state,
 * it will try again to acquire the mutex.
 * If the mutex is already locked by this thread, it is considered an error.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_mutex_lock() {
    int val = sigsetjmp(threads[uthread_get_tid()]->env, 1);
    if (is_mutex_locked()) {

        //mutex is already locked by this thread
        if (lock_thread == uthread_get_tid()) {
            print_library_error("could not lock mutex. This thready already locked it.");
            return -1;
        }

        //mutex is already locked by different thread
        block_alarm();
        move_current_to_mutex_blocked(); // save context, add id to blocked list and remove from running
        round_robin(); // find next ready thread, remove from ready list and update running
        return 0;
    }
    //lock mutex

    lockMutex();
    return 0;
}


/*
 * Description: This function releases a mutex.
 * If there are blocked threads waiting for this mutex,
 * one of them (no matter which one) moves to READY state.
 * If the mutex is already unlocked, it is considered an error.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_mutex_unlock(){
    //mutex already unlocked or tid is different then lock tid
    if (!is_mutex_locked()) {
        print_library_error("could not unlock mutex. Mutex is already unlocked.");
        return -1;
    }
    if (uthread_get_tid() != lock_thread) {
        print_library_error("could not unlock mutex. This mutex was locked by another thread.");
    return -1;
}

    // unlock (current thread is the lock thread)
    unlockMutex();

    // remove a thread from mutex_blocked to ready
    if (mutex_blocked.size() != 0){
        auto it = mutex_blocked.begin();
        mutex_blocked.erase(it);
        ready.push_back(*it);
    }
    else if (double_blocked.size() != 0){
        auto it = double_blocked.begin();
        double_blocked.erase(it);
        blocked.push_back(*it);
    }
    return 0;
}


/*
 * Description: This function returns the thread ID of the calling thread.
 * Return value: The ID of the calling thread.
*/
int uthread_get_tid(){
    return running;
}


/*
 * Description: This function returns the total number of quantums since
 * the library was initialized, including the current quantum.
 * Right after the call to uthread_init, the value should be 1.
 * Each time a new quantum starts, regardless of the reason, this number
 * should be increased by 1.
 * Return value: The total number of quantums.
*/
int uthread_get_total_quantums() {
    return total_quantums;
}

/*
 * Description: This function returns the number of quantums the thread with
 * ID tid was in RUNNING state. On the first time a thread runs, the function
 * should return 1. Every additional quantum that the thread starts should
 * increase this value by 1 (so if the thread with ID tid is in RUNNING state
 * when this function is called, include also the current quantum). If no
 * thread with ID tid exists it is considered an error.
 * Return value: On success, return the number of quantums of the thread with ID tid.
 * 			     On failure, return -1.
*/
int uthread_get_quantums(int tid) {

    //tid do not exist
    if (threads.find(tid) == threads.end()) {
        print_library_error("could not fetch quantums. Thread id "+ std::to_string(tid) + " doesnt exist");
        return -1;
    }

    return threads[tid]->quantums;
}


