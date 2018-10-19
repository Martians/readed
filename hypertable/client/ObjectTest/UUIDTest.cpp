#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/time.h>
#include <uuid/uuid.h>
#include <string.h>
#include "UUIDTest.hpp"

UUIDTest::UUIDTest()
{
}

UUIDTest::~UUIDTest()
{
}

void *
UUIDTest::entry(void *args)
{
//    char uuidbuffer[37];
//    uuid_t uuid;
    struct timeval timev; 
    size_t i = 0;
    gettimeofday(&timev, NULL);
    printf("seconds: %u, useconds: %u\r\n", (unsigned int)timev.tv_sec, (unsigned int)timev.tv_usec);
    while(1) {
        //uuid_generate(uuid);
        //uuid_unparse(uuid, uuidbuffer);
        i ++;
        if(i % 300000 == 0) {
            gettimeofday(&timev, NULL);
            printf("300000 uuid seconds: %u, useconds: %u\r\n", (unsigned int)timev.tv_sec, (unsigned int)timev.tv_usec);
        }
    }
    return NULL;
}

    void
UUIDTest::Create()
{
    pthread_t thread_id;
    pthread_attr_t attr;
    memset(&attr, 0, sizeof(pthread_attr_t));
    pthread_attr_init(&attr);

    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_create(&thread_id, &attr, entry, (void*) this);
    pthread_attr_destroy(&attr);

}

