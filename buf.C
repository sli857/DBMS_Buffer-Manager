/*
 * Team number: 39
 * Group Members:
 *   Name: David Li, ID: 908 313 4198
 *   Name: Wei Wei, ID:
 *   Name: Ziqin Shen, ID:
 */

#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)                                              \
    {                                                          \
        if (!(c))                                              \
        {                                                      \
            cerr << "At line " << __LINE__ << ":" << endl      \
                 << "  ";                                      \
            cerr << "This condition should hold: " #c << endl; \
            exit(1);                                           \
        }                                                      \
    }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++)
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
    hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

    clockHand = bufs - 1;
}

BufMgr::~BufMgr()
{

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true)
        {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete[] bufTable;
    delete[] bufPool;
}

/*Allocates a free frame using the clock algorithm; if necessary, writing a
dirty page back to disk. Returns BUFFEREXCEEDED if all buffer frames are pinned,
UNIXERR if the call to the I/O layer returned an error when a dirty page was
being written to disk and OK otherwise.  This private method will get called by
the readPage() and allocPage() methods described below.

Make sure that if the buffer frame allocated has a valid page in it, that you
remove the appropriate entry from the hash table.*/
const Status BufMgr::allocBuf(int &frame)
{
    //count iterations
    int count{0};
    //count pinned frames
    int pinned{0};

    do
    {
        // iterate all frames and all frames are pinned
        if (count == numBufs && pinned == numBufs)
        {
            return BUFFEREXCEEDED;
        }

        // increment
        count++;
        advanceClock();

        // store a pointer to current frame
        BufDesc *target_desc = &bufTable[clockHand];

        // page of the frame is invalid, use directly
        if (!target_desc->valid)
        {
            frame = target_desc->frameNo;
            return OK;
        }

        // checked pinned?
        if (target_desc->pinCnt > 0)
        {
            pinned++;
            continue;
        }

        // check ref? decrement if true
        if (target_desc->refbit)
        {
            target_desc->refbit = false;
            continue;
        }

        // current frame is good to be replaced if code runs up to here

        // check dirty? write back to disk if true
        if (target_desc->dirty)
        {
            // write this page to disk
            auto writeResult = target_desc->file->writePage(target_desc->pageNo, &bufPool[clockHand]);
            if (writeResult != OK)
            {
                return writeResult;
            }
            target_desc->dirty = false;
        }

        // remove from hashtable if there is already a valid page
        if (target_desc->file != nullptr)
        {
            hashTable->remove(target_desc->file, target_desc->pageNo);
        }
        
        // set the parameter frame to current frame
        frame = target_desc->frameNo;
        return OK;
    } while (true);
}

/*First check whether the page is already in the buffer pool by invoking the
lookup() method on the hashtable to get a frame number.  There are two cases to
be handled depending on the outcome of the lookup() call:

Case 1) Page is not in the buffer pool.  Call allocBuf() to allocate a buffer
frame and then call the method file->readPage() to read the page from disk into
the buffer pool frame. Next, insert the page into the hashtable. Finally, invoke
Set() on the frame to set it up properly. Set() will leave the pinCnt for the
page set to 1.  Return a pointer to the frame containing the page via the page
parameter.

Case 2)  Page is in the buffer pool.  In this case set the appropriate refbit,
increment the pinCnt for the page, and then return a pointer to the frame
containing the page via the page parameter.

Returns OK if no errors occurred, UNIXERR if a Unix error occurred,
BUFFEREXCEEDED if all buffer frames are pinned, HASHTBLERROR if a hash table
error occurred.*/
const Status BufMgr::readPage(File *file, const int PageNo, Page *&page)
{

    // look up page from hashtable
    int frameNo{-1};
    auto lookupResult = hashTable->lookup(file, PageNo, frameNo);

    // case 1: not found
    if (lookupResult == HASHNOTFOUND)
    {

        auto allocResult = allocBuf(frameNo);
        if (allocResult == OK)
        {
            ASSERT(frameNo != -1);
            auto readPageResult = file->readPage(PageNo, &bufPool[frameNo]);
            if (readPageResult != OK)
            {
                return readPageResult;
            }
            auto insertResult = hashTable->insert(file, PageNo, frameNo);
            if (insertResult == OK)
            {
                bufTable[frameNo].Set(file, PageNo);
                page = &bufPool[frameNo];
                return OK;
            }
            return insertResult;
        }
        return allocResult;
    }

    // case 2: found
    else if (lookupResult == OK)
    {
        ASSERT(frameNo != -1);
        bufTable[frameNo].refbit = true;
        bufTable[frameNo].pinCnt++;
        page = &bufPool[frameNo];
        return OK;
    }

    return lookupResult;
}

/*
Decrements the pinCnt of the frame containing (file, PageNo) and, if dirty ==
true, sets the dirty bit.  Returns OK if no errors occurred, HASHNOTFOUND if the
page is not in the buffer pool hash table, PAGENOTPINNED if the pin count is
already 0.*/
const Status BufMgr::unPinPage(File *file, const int PageNo,
                               const bool dirty)
{
    // look up page in hastTable
    int frameNo{-1};
    auto lookupResult = hashTable->lookup(file, PageNo, frameNo);

    // not found
    if (lookupResult != OK)
    {
        return HASHNOTFOUND;
    }
    ASSERT(frameNo != -1);

    // not pinned
    if (bufTable[frameNo].pinCnt == 0)
    {
        return PAGENOTPINNED;
    }

    // decrement pin
    bufTable[frameNo].pinCnt--;

    // update dirty
    if (dirty == true)
    {
        bufTable[frameNo].dirty = true;
    }
    return OK;
}

/*This call is kind of weird.  The first step is to to allocate an empty page in
the specified file by invoking the file->allocatePage() method. This method will
return the page number of the newly allocated page.  Then allocBuf() is called
to obtain a buffer pool frame.  Next, an entry is inserted into the hash table
and Set() is invoked on the frame to set it up properly.  The method returns
both the page number of the newly allocated page to the caller via the pageNo
parameter and a pointer to the buffer frame allocated for the page via the page
parameter. Returns OK if no errors occurred, UNIXERR if a Unix error occurred,
BUFFEREXCEEDED if all buffer frames are pinned and HASHTBLERROR if a hash table
error occurred. */
const Status BufMgr::allocPage(File *file, int &pageNo, Page *&page)
{
    // allocate page
    int pageNum{-1};
    auto allocatePageresult = file->allocatePage(pageNum);
    if (allocatePageresult != OK)
    {
        return allocatePageresult;
    }
    ASSERT(pageNum != -1);

    // alloc buf frame
    int frameNo{-1};
    auto allocBufResult = allocBuf(frameNo);
    if (allocBufResult != OK)
    {
        return allocBufResult;
    }
    ASSERT(frameNo != -1);

    // insert into hashtable
    auto insertResult = hashTable->insert(file, pageNum, frameNo);
    if (insertResult != OK)
    {
        return insertResult;
    }
    bufTable[frameNo].Set(file, pageNum);
    pageNo = pageNum;
    page = &bufPool[frameNo];
    return OK;
}

const Status BufMgr::disposePage(File *file, const int pageNo)
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File *file)
{
    Status status;

    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &(bufTable[i]);
        if (tmpbuf->valid == true && tmpbuf->file == file)
        {

            if (tmpbuf->pinCnt > 0)
                return PAGEPINNED;

            if (tmpbuf->dirty == true)
            {
#ifdef DEBUGBUF
                cout << "flushing page " << tmpbuf->pageNo
                     << " from frame " << i << endl;
#endif
                if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
                                                      &(bufPool[i]))) != OK)
                    return status;

                tmpbuf->dirty = false;
            }

            hashTable->remove(file, tmpbuf->pageNo);

            tmpbuf->file = NULL;
            tmpbuf->pageNo = -1;
            tmpbuf->valid = false;
        }

        else if (tmpbuf->valid == false && tmpbuf->file == file)
            return BADBUFFER;
    }

    return OK;
}

void BufMgr::printSelf(void)
{
    BufDesc *tmpbuf;

    cout << endl
         << "Print buffer...\n";
    for (int i = 0; i < numBufs; i++)
    {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char *)(&bufPool[i])
             << "\tpinCnt: " << tmpbuf->pinCnt;

        if (tmpbuf->refbit == true)
        {
            cout << "\tref";
        }
        else{
            cout << "\t   ";
        }
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    }
}
