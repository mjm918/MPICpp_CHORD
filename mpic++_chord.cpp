//
// Created by julfikar on 6/14/18.
//
#include <iostream>
#include <cstdint>
#include <cstring>
#include <unistd.h>
#include <map>
#include <sys/time.h>
#include <mpi.h>
#include <cmath>

#if __cplusplus >= 201103L || (defined(_MSC_VER) && _MSC_VER >= 1900)
#include <cstdint>
#include <cstdlib>
#else
#include <stdint.h>
#include <stdlib.h>
#endif

#define print cout <<
#define error cerr <<
#define endf << endl
#define _ <<
#define TERMINATE "terminate"

using namespace std;

enum {
    mBIT = 7,
    CHAR_SIZE = 30,
    CHORDSIZE = 128,
    MPI_CHORD_RS = 200,
    MPI_CHORD_ACTION = 201,
    MPI_CHORD_INSERT = 202,
    MPI_CHORD_DELETE = 203,
    MPI_CHORD_SEARCH = 204,
    MPI_CHORD_PRINT = 205,
};

long long getUnixTime(){
#if __cplusplus >= 201103L || (defined(_MSC_VER) && _MSC_VER >= 1900)
    struct timeval ut{};
    gettimeofday(&ut, nullptr);
    return 1000000 * ut.tv_sec + ut.tv_usec;
#else
    struct timeval ut;
    gettimeofday(&ut, NULL);
    return 1000000 * ut.tv_sec + ut.tv_usec;
#endif
}

uint16_t charToHash (char *str, int cordSize){
    uint16_t hashKey = 0;
    for(size_t i = 0; i < strlen(str); i++){
        hashKey = uint16_t (((hashKey << 5 + hashKey) ^ (int)str[i]) % cordSize);
    }
    return hashKey;
}

int main(int argc, char **argv) {

    // init mpi
    #if __cplusplus >= 201103L || (defined(_MSC_VER) && _MSC_VER >= 1900)
        MPI_Init(nullptr, nullptr);
    #else
        MPI_Init(NULL, NULL);
    #endif
    // get world size
    int _world_size;
    MPI_Comm_size(MPI_COMM_WORLD,&_world_size);
    // get rank
    int _rank;
    MPI_Comm_rank(MPI_COMM_WORLD,&_rank);

    if(_world_size != 10 && _rank == 0){
        error "[EXCEPTION] Program must run with 10 processes" endf;
        MPI_Finalize();
        return 0;
    }

    int _peer = -1;
    int _fingerTable[mBIT] = {-1,-1,-1,-1,-1,-1,-1};
    int *_connectedPeers = new int[_world_size];

    map<int,char*>container;

    srand(getUnixTime());

    if(_rank == 0){
        print "CHORD Simulation Program" endf;
        print "Start with size " _ CHORDSIZE _ " and " _ _world_size _ " peers" endf;

        size_t size = CHORDSIZE / _world_size;

        for(int index = 0; index < _world_size; index++){
            int _id = (int)(index * size + (rand()%size));

            if(index == 0){
                _id = 0;
            }

            _connectedPeers[index] = _id;
        }

        for(int index = 1; index < _world_size; index++){
            MPI_Send(_connectedPeers,_world_size,MPI_INT,index, MPI_CHORD_RS,MPI_COMM_WORLD);
        }

        print "Available peers : " endf;

        for(int index = 0; index < _world_size; index++){
            if(index != 0){
                print ",";
            }
            print _connectedPeers[index];
        }
        print " " endf;

        // building finger table
        for (int index = 0; index < mBIT; index++){
            int node = ((int)pow(2,index)+_peer);
            for(int i = 0; i < _world_size; i++){
                if(node > _connectedPeers[_world_size-1]){
                    if(_connectedPeers[i]+CHORDSIZE>=node){
                        _fingerTable[index] = _connectedPeers[i];
                        break;
                    }
                }else{
                    if(_connectedPeers[i]>=node){
                        _fingerTable[index] = _connectedPeers[i];
                        break;
                    }
                }
            }
        }

        _peer = _connectedPeers[_rank];

        // all peers have created finger table
        MPI_Barrier(MPI_COMM_WORLD);

        // available functions
        int ask = 0;
        while (ask != 5){
            print "Choose a task : (1)Insert, (2)Delete, (3)Search, (4)Print, (5)Quit" endf;
            cin >> ask;
            // i = 1 cause id 0 is performing
            for(int i = 1; i < _world_size; i++){
                MPI_Send(&ask,1,MPI_INT,i,MPI_CHORD_ACTION,MPI_COMM_WORLD);
            }
            // insert
            if(ask == 1){
                char dataTobeHashed[CHAR_SIZE];
                print "Enter data :" endf;
                cin >> dataTobeHashed;

                uint16_t hashed = charToHash(dataTobeHashed,CHORDSIZE);

                if(hashed > _connectedPeers[_world_size-1]){
                    container.insert(pair<int,char*>(hashed,dataTobeHashed));
                    print "\"" _ dataTobeHashed _ "\" hashed to " _ hashed _ " and store at peer " _ _peer endf;
                }else{
                    int node = 0;

                    for(int i = 0, j = 1; i < mBIT-1; i++,j++){
                        if(hashed >= _fingerTable[i] && hashed < _fingerTable[j]){
                            node = _fingerTable[i];
                            break;
                        }
                    }
                    for(int index = 0; index < _world_size; index++){
                        if(_connectedPeers[index] == node){
                            node = index+1;
                            break;
                        }
                    }

                    MPI_Send(dataTobeHashed,CHAR_SIZE,MPI_CHAR,node,MPI_CHORD_INSERT,MPI_COMM_WORLD);
                    char returned[CHAR_SIZE];
                    MPI_Recv(returned,CHAR_SIZE,MPI_CHAR,MPI_ANY_SOURCE,MPI_CHORD_INSERT,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                }
            }
            // delete
            if(ask == 2){
                char dataTobeHashed[CHAR_SIZE];

                print "Enter data to delete" endf;
                cin >> dataTobeHashed;

                uint16_t hashed = charToHash(dataTobeHashed,CHORDSIZE);

                if(hashed > _connectedPeers[_world_size-1]){
                    map<int,char*>::iterator id = container.find(hashed);
                    if(id != container.end()){
                        container.erase(hashed);
                        print "\"" _ id->second _ "\" was deleted from peer id " _ _peer endf;
                    }else{
                        print "Data is not found" endf;
                    }
                }else{
                    int node = 0;

                    for(int i = 0, j = 1; i < mBIT-1; i++,j++){
                        if(hashed >= _fingerTable[i] && hashed < _fingerTable[j]){
                            node = _fingerTable[i];
                            break;
                        }
                    }
                    for(int index = 0; index < _world_size; index++){
                        if(_connectedPeers[index] == node){
                            node = index+1;
                            break;
                        }
                    }

                    MPI_Send(dataTobeHashed,CHAR_SIZE,MPI_CHAR,node,MPI_CHORD_DELETE,MPI_COMM_WORLD);
                    char returned[CHAR_SIZE];
                    MPI_Recv(returned,CHAR_SIZE,MPI_CHAR,MPI_ANY_SOURCE,MPI_CHORD_DELETE,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                }
            }
            // search
            if(ask == 3){
                char dataTobeHashed[CHAR_SIZE];
                print "Enter search string :" endf;
                cin >> dataTobeHashed;

                uint16_t hashed = charToHash(dataTobeHashed,CHORDSIZE);

                if(hashed > _connectedPeers[_world_size-1]){
                    map<int,char*>::iterator id = container.find(hashed);
                    if(id != container.end()){
                        print "\"" _ id->second _ "\" was found at peer id " _ _peer endf;
                    }else{
                        print "Data is not found" endf;
                    }
                }else{
                    int node = 0;

                    for(int i = 0, j = 1; i < mBIT-1; i++,j++){
                        if(hashed >= _fingerTable[i] && hashed < _fingerTable[j]){
                            node = _fingerTable[i];
                            break;
                        }
                    }
                    for(int index = 0; index < _world_size; index++){
                        if(_connectedPeers[index] == node){
                            node = index+1;
                            break;
                        }
                    }

                    MPI_Send(dataTobeHashed,CHAR_SIZE,MPI_CHAR,node,MPI_CHORD_SEARCH,MPI_COMM_WORLD);
                    char returned[CHAR_SIZE];
                    MPI_Recv(returned,CHAR_SIZE,MPI_CHAR,MPI_ANY_SOURCE,MPI_CHORD_SEARCH,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                }
            }
            //print
            if(ask == 4){
                int _peerTobePrinted;
                print "Enter peer number to print :" endf;
                cin >> _peerTobePrinted;
                if(_peerTobePrinted == _peer){
                    print "Peer " _ _peer _ " finger table" endf;
                    print "===============================" endf;
                    print "Index\tNode" endf;

                    for(int index = 0; index < mBIT; index++){
                        print index+1 _ "\t" _ _fingerTable[index] endf;
                    }

                    print "Peer " _ _peer _ " Data " endf;
                    print "=============================" endf;
                    print "Key\tData" endf;

                    if(container.empty()){
                        print "No data is found" endf;
                    }else{
                        for(map<int,char*>::iterator id = container.begin();id != container.end(); id++){
                            print id->first _ "\t" _ id->second endf;
                        }
                    }
                }else{
                    int node = -1;
                    for(int index = 0; index < _world_size; index++){
                        if(_connectedPeers[index] == _peerTobePrinted){
                            node = index;
                            break;
                        }
                    }

                    if(node == -1){
                        print "No peer found" endf;
                    }else{
                        MPI_Send(&_peerTobePrinted,1,MPI_INT,node,MPI_CHORD_PRINT,MPI_COMM_WORLD);
                        int returned;
                        MPI_Recv(&returned,1,MPI_INT,node,MPI_CHORD_PRINT,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                    }
                }
            }
        }
    }else{
        // getting all peerid from p0
        MPI_Recv(_connectedPeers,_world_size,MPI_INT,0,MPI_CHORD_RS,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        // getting own peerid
        _peer = _connectedPeers[_rank];

        // constructing finger table
        for(int index = 0; index < mBIT; index++){
            int node = ((int)pow(2,index)+_peer);
            for(int i = 0; i < _world_size; i++){
                if(node > _connectedPeers[_world_size-1]){
                    if(_connectedPeers[i]+CHORDSIZE>=node){
                        _fingerTable[index] = _connectedPeers[i];
                        break;
                    }
                }else{
                    if(_connectedPeers[i]>=node){
                        _fingerTable[index] = _connectedPeers[i];
                        break;
                    }
                }
            }
        }

        MPI_Barrier(MPI_COMM_WORLD);

        int ask = 0;

        while (ask!=5){
            int flag = 0;

            MPI_Request mReq;
            MPI_Irecv(&ask,1,MPI_INT,MPI_ANY_SOURCE,MPI_CHORD_ACTION,MPI_COMM_WORLD,&mReq);

            int counter = 0;

            do{
                usleep(100000);
                counter++;
                if(counter>10){
                    MPI_Cancel(&mReq);
                    break;
                }
                MPI_Test(&mReq,&flag,MPI_STATUS_IGNORE);
            }while (!flag);

            if(flag){
                //insert
                if(ask == 1){
                    bool run = false;
                    while(!run){
                        char hashedData[CHAR_SIZE];

                        int thisFlag = 0, thisCounter = 0;
                        // get hashed data
                        MPI_Request thisMreq;
                        MPI_Irecv(&hashedData,CHAR_SIZE,MPI_CHAR,MPI_ANY_SOURCE,MPI_CHORD_INSERT,MPI_COMM_WORLD,&thisMreq);
                        do{
                            usleep(100000);
                            thisCounter++;
                            if(thisCounter>10){
                                MPI_Cancel(&thisMreq);
                                break;
                            }
                            MPI_Test(&thisMreq,&thisFlag,MPI_STATUS_IGNORE);
                        }while (!thisFlag);

                        if(thisFlag){
                            if(strcmp(hashedData,TERMINATE) == 0){
                                run = true;
                            }else{
                                uint16_t hashed = charToHash(hashedData,CHORDSIZE);
                                if(hashed < _fingerTable[0] && hashed <= _peer){
                                    container.insert(pair<int,char*>(hashed,hashedData));
                                    print "\"" _ hashedData _ "\" hashed to " _ hashed _ " and stored at peer " _ _peer endf;

                                    run = true;

                                    for(int index = 0; index < _world_size; index++){
                                        char returned[CHAR_SIZE] = TERMINATE;
                                        if (index != _rank){
                                            MPI_Send(returned,CHAR_SIZE,MPI_CHAR,index,MPI_CHORD_INSERT,MPI_COMM_WORLD);
                                        }
                                    }

                                }else{
                                    int node = 0;
                                    for(int i = 0, j = 1; i < mBIT-1; i++,j++){
                                        if(hashed >= _fingerTable[i] && hashed < _fingerTable[j]){
                                            node = _fingerTable[i];
                                            break;
                                        }
                                    }
                                    for(int i = 0; i < _world_size; i++){
                                        if(_connectedPeers[i] == node){
                                            node = i+1;
                                            break;
                                        }
                                    }

                                    MPI_Send(hashedData,CHAR_SIZE,MPI_CHAR,node,MPI_CHORD_INSERT,MPI_COMM_WORLD);
                                }
                            }
                        }
                    }
                }
                if(ask == 2){
                    bool run = false;

                    while(!run){
                        char hashedData[CHAR_SIZE];
                        int thisFlag = 0, thisCounter = 0;

                        MPI_Request thisMreq;
                        MPI_Irecv(&hashedData,CHAR_SIZE,MPI_CHAR,MPI_ANY_SOURCE,MPI_CHORD_DELETE,MPI_COMM_WORLD,&thisMreq);
                        do{
                            usleep(100000);
                            thisCounter++;
                            if(thisCounter>10){
                                MPI_Cancel(&thisMreq);
                                break;
                            }
                            MPI_Test(&thisMreq,&thisFlag,MPI_STATUS_IGNORE);
                        }while (!thisFlag);

                        if(thisFlag) {
                            if (strcmp(hashedData, TERMINATE) == 0) {
                                run = true;
                            }else{
                                uint16_t hashed = charToHash(hashedData,CHORDSIZE);
                                if(hashed < _fingerTable[0] && hashed <= _peer){
                                    map<int,char*>::iterator id = container.find(hashed);
                                    if(id != container.end()){
                                        container.erase(hashed);
                                        print "\"" _ id->second _ "\" was deleted from peer " << _peer endf;
                                    }else{
                                        print "Data is not found" endf;
                                    }
                                    run = true;

                                    for(int index = 0; index < _world_size; index++){
                                        char returned[CHAR_SIZE] = TERMINATE;
                                        if (index != _rank){
                                            MPI_Send(returned,CHAR_SIZE,MPI_CHAR,index,MPI_CHORD_DELETE,MPI_COMM_WORLD);
                                        }
                                    }
                                }else{
                                    int node = 0;
                                    for(int i = 0, j = 1; i < mBIT-1; i++,j++){
                                        if(hashed >= _fingerTable[i] && hashed < _fingerTable[j]){
                                            node = _fingerTable[i];
                                            break;
                                        }
                                    }
                                    for(int i = 0; i < _world_size; i++){
                                        if(_connectedPeers[i] == node){
                                            node = i+1;
                                            break;
                                        }
                                    }
                                    MPI_Send(hashedData,CHAR_SIZE,MPI_CHAR,node,MPI_CHORD_DELETE,MPI_COMM_WORLD);
                                }
                            }
                        }
                    }
                }
                if(ask == 3){
                    bool run = false;

                    while(!run){
                        char hashedData[CHAR_SIZE];
                        int thisFlag = 0, thisCounter = 0;

                        MPI_Request thisMreq;
                        MPI_Irecv(&hashedData,CHAR_SIZE,MPI_CHAR,MPI_ANY_SOURCE,MPI_CHORD_SEARCH,MPI_COMM_WORLD,&thisMreq);
                        do{
                            usleep(100000);
                            thisCounter++;
                            if(thisCounter>10){
                                MPI_Cancel(&thisMreq);
                                break;
                            }
                            MPI_Test(&thisMreq,&thisFlag,MPI_STATUS_IGNORE);
                        }while (!thisFlag);

                        if(thisFlag) {
                            if (strcmp(hashedData, TERMINATE) == 0) {
                                run = true;
                            }else{
                                uint16_t hashed = charToHash(hashedData,CHORDSIZE);
                                if(hashed < _fingerTable[0] && hashed <= _peer){
                                    map<int,char*>::iterator id = container.find(hashed);
                                    if(id != container.end()){
                                        print ">" _ _peer endf;
                                        print "\"" _ id->second _ "\" was found at peer " _ _peer endf;
                                    }else{
                                        print "Data is not found" endf;
                                    }
                                    run = true;

                                    for (int index = 0; index < _world_size; index++){
                                        char returned[CHAR_SIZE] = TERMINATE;
                                        if (index != _rank){
                                            MPI_Send(returned,CHAR_SIZE,MPI_CHAR,index,MPI_CHORD_SEARCH,MPI_COMM_WORLD);
                                        }
                                    }
                                }else{
                                    int node = 0;
                                    for(int i = 0, j = 1; i < mBIT-1; i++,j++){
                                        if(hashed >= _fingerTable[i] && hashed < _fingerTable[j]){
                                            node = _fingerTable[i];
                                            break;
                                        }
                                    }
                                    for(int i = 0; i < _world_size; i++){
                                        if(_connectedPeers[i] == node){
                                            node = i+1;
                                            break;
                                        }
                                    }
                                    MPI_Send(hashedData,CHAR_SIZE,MPI_CHAR,node,MPI_CHORD_SEARCH,MPI_COMM_WORLD);
                                }
                            }
                        }
                    }
                }
                if(ask == 4){
                    bool run = false;

                    while (!run){
                        int _peerTobePrinted;
                        int thisFlag = 0, thisCounter = 0;

                        MPI_Request thisMreq;
                        MPI_Irecv(&_peerTobePrinted,1,MPI_INT,MPI_ANY_SOURCE,MPI_CHORD_PRINT,MPI_COMM_WORLD,&thisMreq);

                        do{
                            usleep(100000);
                            thisCounter++;
                            if(thisCounter>10){
                                MPI_Cancel(&thisMreq);
                                break;
                            }
                            MPI_Test(&thisMreq,&thisFlag,MPI_STATUS_IGNORE);
                        }while (!thisFlag);

                        if(thisFlag){
                            if(_peerTobePrinted == -2){
                                run = true;
                            }else{
                                if(_peerTobePrinted == _peer){
                                    print "Peer " _ _peer _ " finger table" endf;
                                    print "===============================" endf;
                                    print "Index\tNode" endf;

                                    for (int index = 0; index < mBIT; index++){
                                        print index+1 _ "\t" _ _fingerTable[index] endf;
                                    }

                                    print "Peer " _ _peer _ " Data " endf;
                                    print "=============================" endf;
                                    print "Key\tData" endf;

                                    if(container.empty()){
                                        print "No data is found" endf;
                                    }else{
                                        for(map<int,char*>::iterator id = container.begin();id != container.end(); id++){
                                            print id->first _ "\t" _ id->second endf;
                                        }
                                    }

                                    for(int i = 0; i < _world_size; i++){
                                        int terminate = -2;
                                        if(i != _rank){
                                            MPI_Send(&terminate,1,MPI_INT,i,MPI_CHORD_PRINT,MPI_COMM_WORLD);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if(_rank == 0){
        print " " endf;
    }

    MPI_Finalize();
}

