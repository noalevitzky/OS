#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <utility> //std::pair
#include <vector>  //std::vector
#include <cstdlib>
#include <cstdio>
#include <utility>
#include <semaphore.h>
#include <algorithm>
#include <iostream>
#include <unistd.h>

struct JobContext;
void *mapSortShuffleReduce(void *context);
void *mapSortReduce(void *context);

const char* sys_err = "system error: ";

// BARRIER

class Barrier {
public:
    Barrier(int numThreads)
    : mutex(PTHREAD_MUTEX_INITIALIZER)
    , cv(PTHREAD_COND_INITIALIZER)
    , count(0)
    , numThreads(numThreads)
    { }


    ~Barrier()
    {
        if (pthread_mutex_destroy(&mutex) != 0) {
            fprintf(stderr, "%s%s\n", sys_err ,"[[Barrier]] error on pthread_mutex_destroy");
            exit(1);
        }
        if (pthread_cond_destroy(&cv) != 0){
            fprintf(stderr, "%s%s\n", sys_err ,"[[Barrier]] error on pthread_cond_destroy");
            exit(1);
        }
    }

    void barrier()
    {
        if (pthread_mutex_lock(&mutex) != 0){
            fprintf(stderr, "%s%s\n", sys_err ,"[[Barrier]] error on pthread_mutex_lock");
            exit(1);
        }
        if (++count < numThreads) {
            if (pthread_cond_wait(&cv, &mutex) != 0){
                fprintf(stderr, "%s%s\n", sys_err ,"[[Barrier]] error on pthread_cond_wait");
                exit(1);
            }
        } else {
            count = 0;
            if (pthread_cond_broadcast(&cv) != 0) {
                fprintf(stderr, "%s%s\n", sys_err ,"[[Barrier]] error on pthread_cond_broadcast");
                exit(1);
            }
        }
        if (pthread_mutex_unlock(&mutex) != 0) {
            fprintf(stderr, "%s%s\n", sys_err ,"[[Barrier]] error on pthread_mutex_unlock");
            exit(1);
        }
    }


private:
    pthread_mutex_t mutex;
    pthread_cond_t cv;
    int count;
    int numThreads;
};

//end of barrier


typedef struct ThreadContext{
    int threadID;
    std::vector<IntermediatePair> IntermediatePairs;
    std::vector<std::pair<K3*, V3*>> OutputPairs;
    JobContext &myJob;

    ThreadContext(int id, JobContext &job): threadID(id), myJob(job){
        
    };
} ThreadContext;

typedef struct JobContext{

    const MapReduceClient &client;
    const InputVec &inputVec;
    OutputVec &outputVec;
    std::atomic<int> stage;
    Barrier barrier;
    pthread_t* threads;
    std::vector<ThreadContext*> contexts;
    std::atomic<int> inputVecCounter;
    const int numThreads;

    //map data
    std::atomic<int> intermediateCounter;
    std::vector<K2*> uniqueKeys;
    pthread_mutex_t uniqueKeyMutex = PTHREAD_MUTEX_INITIALIZER;


    //shuffle data
    std::vector<IntermediateVec> shuffleOutput;
    std::atomic<int> shuffleCounter;

    //reduce data
    std::atomic<int> reduceIndex;
    std::atomic<int> reduceCounter;
    pthread_mutex_t reduceMutex = PTHREAD_MUTEX_INITIALIZER;
    std::atomic<int> outputCounter;
    pthread_mutex_t outputMutex = PTHREAD_MUTEX_INITIALIZER;

    //other
    pthread_mutex_t waitForJobMutex = PTHREAD_MUTEX_INITIALIZER;
    bool wasJoinCalled;

    JobContext(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel):
    client(client), inputVec(inputVec), outputVec(outputVec), barrier(multiThreadLevel), inputVecCounter(0),
    intermediateCounter(0), shuffleCounter(0), reduceIndex(0), reduceCounter(0), outputCounter(0),
    numThreads(multiThreadLevel), wasJoinCalled(false)
    {
        
        stage.store(UNDEFINED_STAGE);
        threads = new pthread_t[multiThreadLevel];

        for (int i = 0; i < multiThreadLevel; ++i) {
            ThreadContext* t = new ThreadContext(i, *this);
            contexts.push_back(t);
        }

        // Create thread 0:
        
        if (pthread_create(&threads[0], NULL, mapSortShuffleReduce, contexts[0]) != 0){
            fprintf(stderr, "%s%s\n", sys_err ,"error on pthread_create");
            exit(1);
        }

        // Create other threads:
        for (int i = 1; i < multiThreadLevel; ++i) {
            
            if (pthread_create(&threads[i], NULL, mapSortReduce, contexts[i]) != 0){
                fprintf(stderr, "%s%s\n", sys_err ,"error on pthread_create");
                exit(1);
            }
        }

    };


} JobContext;

struct {
    bool operator()(const IntermediatePair a, const IntermediatePair b){
        return *(a.first) < *(b.first);
    }
} IntermediatePairComperator;

struct {
    bool operator()(const K2* a, const K2* b){
        return *a < *b;
    }
}K2PointerComperator;

bool isKeyInUnique(ThreadContext* thisContext, K2* key){
    for (auto i =thisContext->myJob.uniqueKeys.begin() ; i != thisContext->myJob.uniqueKeys.end(); i++)
    {
        if (!(**i < *key) && !(*key < **i)) { return true; }
    }
    return false;
}

void emit2 (K2* key, V2* value, void* context) {
    
    ThreadContext* thisContext = (ThreadContext*) context;
    thisContext->IntermediatePairs.push_back(std::make_pair(key, value));
    pthread_mutex_lock(&(thisContext->myJob.uniqueKeyMutex));
    if (!isKeyInUnique(thisContext, key)) { thisContext->myJob.uniqueKeys.push_back(key); }
    pthread_mutex_unlock(&(thisContext->myJob.uniqueKeyMutex));

    thisContext->myJob.intermediateCounter++;
}

void emit3 (K3* key, V3* value, void* context){
    
    ThreadContext* thisContext = (ThreadContext*) context;
    if (pthread_mutex_lock(&(thisContext->myJob.outputMutex)) != 0) {
        fprintf(stderr, "%s%s\n", sys_err ,"error on pthread_mutex_lock");
        exit(1);
    }
    thisContext->myJob.outputVec.push_back(std::make_pair(key, value));
    if (pthread_mutex_unlock(&(thisContext->myJob.outputMutex))){
        fprintf(stderr, "%s%s\n", sys_err ,"error on pthread_mutex_unlock");
        exit(1);
    }
    thisContext->myJob.outputCounter++;
}

void map(void *context){
    ThreadContext* thisContext = (ThreadContext*) context;
    
    thisContext->myJob.stage.store(MAP_STAGE);
    int size = ((thisContext->myJob).inputVec).size();
    while (thisContext->myJob.inputVecCounter.load() < size)
    {
        int index = thisContext->myJob.inputVecCounter++;
        K1 *key = ((thisContext->myJob).inputVec[index]).first;
        V1 *val = thisContext->myJob.inputVec[index].second;
        thisContext->myJob.client.map(key, val, context);
    }
}

void sort(ThreadContext *context){
    
    std::sort(context->IntermediatePairs.begin(), context->IntermediatePairs.end(), IntermediatePairComperator);
//    
}

void barrier(ThreadContext *context){
    
    context->myJob.barrier.barrier();
//    

}

void shuffle(ThreadContext *context){
    
    context->myJob.stage.store(SHUFFLE_STAGE);
    std::sort(context->myJob.uniqueKeys.begin(), context->myJob.uniqueKeys.end(), K2PointerComperator);
    while (context->myJob.shuffleCounter.load() < context->myJob.intermediateCounter.load()
    && context->myJob.uniqueKeys.size() > 0){
        K2* curKey = context->myJob.uniqueKeys.back(); // Take last unique key
        std::vector<IntermediatePair> curVec;
        for (int i=0; i < context->myJob.numThreads; i++)
        {
            ThreadContext* thread_context = context->myJob.contexts.at(i);
            while (!thread_context->IntermediatePairs.empty() &&
                    !(*thread_context->IntermediatePairs.back().first < *curKey) &&
                    !(*curKey < *thread_context->IntermediatePairs.back().first)){
                        curVec.push_back(thread_context->IntermediatePairs.back());
                        thread_context->IntermediatePairs.pop_back();
                        context->myJob.shuffleCounter++;
            }
        }
        context->myJob.shuffleOutput.push_back(curVec);
        context->myJob.uniqueKeys.pop_back();
    }
    context->myJob.uniqueKeys.clear();
}

void reduce(void *context) {


    ThreadContext *myContext = (ThreadContext *) context;
//    sem_wait(&context->myJob.shuffle_semaphore); // wait till shuffle ends
    myContext->myJob.stage.store(REDUCE_STAGE);
    while (myContext->myJob.reduceIndex.load() < (int) myContext->myJob.shuffleOutput.size() &&
           myContext->myJob.reduceCounter.load() < myContext->myJob.shuffleCounter.load()) {
        int index = myContext->myJob.reduceIndex++;
        if (pthread_mutex_lock(&(myContext->myJob.reduceMutex)) != 0) {
            fprintf(stderr, "%s%s\n", sys_err, "error on pthread_mutex_lock");
            exit(1);
        }
        IntermediateVec val = myContext->myJob.shuffleOutput[index];
        myContext->myJob.client.reduce(&val, context);
        myContext->myJob.reduceCounter += (val.size());
        if (pthread_mutex_unlock(&(myContext->myJob.reduceMutex)) != 0) {
            fprintf(stderr, "%s%s\n", sys_err, "error on pthread_mutex_unlock");
            exit(1);
        }
    }
}


void *mapSortReduce(void *context){
    ThreadContext *myContext = (ThreadContext*) context;
    
    map(context);
    sort(myContext);
    barrier(myContext);
    barrier(myContext);
    reduce(context);
}

void *mapSortShuffleReduce(void *context){
    ThreadContext *myContext = (ThreadContext*) context;
//    
    map(myContext);
    sort(myContext);
    barrier(myContext);
    shuffle(myContext);
    barrier(myContext);
    reduce(context);
//    
}

/**
 * This function starts running the MapReduce algorithm (with several
threads) and returns a JobHandle.
 * @param client
 * @param inputVec
 * @param outputVec
 * @param multiThreadLevel
 * @return
 */
JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
//    
    JobHandle job = new JobContext(client, inputVec, outputVec, multiThreadLevel);
//    
    return job;
}

/**
 * Gets JobHandle returned by startMapReduceFramework and waits until it is finished.
 * @param job
 */
void waitForJob(JobHandle job){
    
    JobContext* thisJob= (JobContext*) job;

    if (pthread_mutex_lock(&(thisJob->waitForJobMutex)) != 0) {
        fprintf(stderr, "%s%s\n", sys_err ,"error on pthread_mutex_lock");
        exit(1);
    }
    if (!thisJob->wasJoinCalled) {
        thisJob->wasJoinCalled = true;
        for (int i = 0; i < thisJob->numThreads; i++)
        {
            pthread_t* t = (thisJob->threads) + i;
            void* val = NULL;
            int v = pthread_join(*t, &val);
            if (v != 0){
                fprintf(stderr, "%s%s%d%s\n", sys_err ,"error on pthread_join ", i, (char*)val);
                exit(1);
            }
        }
    }
    if (pthread_mutex_unlock(&(thisJob->waitForJobMutex)) != 0) {
        fprintf(stderr, "%s%s\n", sys_err ,"error on pthread_mutex_unlock");
        exit(1);
    }


}

void setStage(JobHandle job, stage_t newStage)
{
    JobContext* thisJob= (JobContext*) job;
    thisJob->stage = newStage;
}

/**
 * This function gets a JobHandle and updates the state of the job into the given JobState struct.
 * @param job
 * @param state
 */
void getJobState(JobHandle job, JobState* state){
    

    JobContext* thisJob= (JobContext*) job;

    switch (thisJob->stage.load())
    {
        case UNDEFINED_STAGE:
            state->stage = UNDEFINED_STAGE;
            state->percentage = 0;
            break;

        case MAP_STAGE:
            state->stage = MAP_STAGE;
            state->percentage = (float) (thisJob->inputVecCounter.load()/thisJob->inputVec.size()) * 100;

            break;

        case SHUFFLE_STAGE:
            state->stage = SHUFFLE_STAGE;
            state->percentage = (float) (thisJob->shuffleCounter.load()/thisJob->intermediateCounter.load()) * 100;

            break;

        case REDUCE_STAGE:
            state->stage = REDUCE_STAGE;
            state->percentage = (float) (thisJob->reduceCounter.load()/thisJob->shuffleCounter.load()) * 100;

            break;
    }
}

/**
 * Releasing all resource of a job.
 * @param job
 */
void closeJobHandle(JobHandle job){
    

    waitForJob(job);

    JobContext* thisJob= (JobContext*) job;

    for (auto p : thisJob->contexts){
        p->IntermediatePairs.clear();

        for (auto j: p->OutputPairs){
            delete j.first;
            delete j.second;
        }
        p->OutputPairs.clear();
    }

    for (auto p : thisJob->contexts){
        delete p;
    }
    thisJob->contexts.clear();
    thisJob->uniqueKeys.clear();

    // Destroy mutexes
    if (pthread_mutex_destroy(&(thisJob->reduceMutex)) !=0){
        fprintf(stderr, "%s%s\n", sys_err ,"error on pthread_mutex_destroy");
        exit(1);
    }
    if (pthread_mutex_destroy(&(thisJob->outputMutex)) !=0){
        fprintf(stderr, "%s%s\n", sys_err ,"error on pthread_mutex_destroy");
        exit(1);
    }

    if (pthread_mutex_destroy(&(thisJob->waitForJobMutex)) !=0){
        fprintf(stderr, "%s%s\n", sys_err ,"error on pthread_mutex_destroy");
        exit(1);
    }

    if (pthread_mutex_destroy(&(thisJob->uniqueKeyMutex)) !=0){
        fprintf(stderr, "%s%s\n", sys_err ,"error on pthread_mutex_destroy");
        exit(1);
    }

    delete thisJob;

}
