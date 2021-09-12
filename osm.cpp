#include "osm.h"
#include <sys/time.h>
#include <cstddef>

/* Empty function which is used for osm_function_time.
 */
static void emptyFunction()
{
    return;
}

static double convertToNano(timeval start, timeval end, unsigned int iterations) {
    return
        // sec to nano
        (((end.tv_sec - start.tv_sec) / (5 * (double) iterations)) * (double) 1000000000) +

        // usec to nano
        (((end.tv_usec - start.tv_usec) / (5 * (double) iterations)) * (double) 1000);
}

/* Time measurement function for a simple arithmetic operation.
   returns time in nano-seconds upon success,
   and -1 upon failure.
   */
double osm_operation_time(unsigned int iterations)
{
    int status;
    struct timeval startTime, endTime;
    int a, b, c, d, e;
    int x = 1, y = 1;
    if (iterations == 0)
    {
        return -1;
    }

    status = gettimeofday(&startTime, NULL);
    if (status == -1)
    {
        return -1;
    }

    for(unsigned int i = 0; i < iterations; i++)
    {
        a = x + y;
        b = x + y;
        c = x + y;
        d = x + y;
        e = x + y;
    }

    status = gettimeofday(&endTime, NULL);
    if (status == -1)
    {
        return -1;
    }

    return convertToNano(startTime, endTime, iterations);

}


/* Time measurement function for an empty function call.
   returns time in nano-seconds upon success,
   and -1 upon failure.
   */
double osm_function_time(unsigned int iterations)
{
    int status;
    struct timeval startTime, endTime;
    if (iterations == 0)
    {
        return -1;
    }

    status = gettimeofday(&startTime, NULL);
    if (status == -1)
    {
        return -1;
    }

    for(unsigned int i = 0; i < iterations; i++)
        {
        emptyFunction();
        emptyFunction();
        emptyFunction();
        emptyFunction();
        emptyFunction();
        }

    status = gettimeofday(&endTime, NULL);
    if (status == -1)
    {
        return -1;
    }

    return convertToNano(startTime, endTime, iterations);

}

/* Time measurement function for an empty trap into the operating system.
   returns time in nano-seconds upon success,
   and -1 upon failure.
   */
double osm_syscall_time(unsigned int iterations)
{
    int status;
    struct timeval startTime, endTime;
    if (iterations == 0)
    {
        return -1;
    }

    status = gettimeofday(&startTime, NULL);
    if (status == -1)
    {
        return -1;
    }

    for(unsigned int i = 0; i < iterations; i++)
        {
        OSM_NULLSYSCALL;
        OSM_NULLSYSCALL;
        OSM_NULLSYSCALL;
        OSM_NULLSYSCALL;
        OSM_NULLSYSCALL;
        }

    status = gettimeofday(&endTime, NULL);
    if (status == -1)
    {
        return -1;
    }

    return convertToNano(startTime, endTime, iterations);
}
