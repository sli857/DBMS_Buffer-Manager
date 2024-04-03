/*
* Team number: 39
* Group Members:
*   Name: David Li, ID: 908 313 4198
*   Name: Wei Wei, ID: 908 354 2150
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

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
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

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table 
    clockHand = bufs - 1;
}

/*
Flushes out all dirty pages and deallocates the buffer pool and the BufDesc table.
*/
BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}

/*
Allocates a free frame using the clock algorithm; if necessary, writing a dirty page back to disk. Returns BUFFEREXCEEDED if all buffer frames are pinned, UNIXERR if the call to the I/O layer returned an error when a dirty page was being written to disk and OK otherwise.
This private method will get called by the readPage() and allocPage() methods.
*/
const Status BufMgr::allocBuf(int & frame) 
{
    unsigned int pointer = clockHand;
    bool allocatedPage = false;

    do {
        //move forward the clock hand
        advanceClock();

        if(pointer == clockHand && allocatedPage){
            return BUFFEREXCEEDED; // all buffer frames are pinned
        }
        
        if(bufTable[clockHand].pinCnt == 0){// whether the frame is pinned
            if(bufTable[clockHand].refbit){// whether the frame is referenced
                bufTable[clockHand].refbit = false; // unset the reference bit
            }else{ // a found free frame
                allocatedPage = true;
                if(bufTable[clockHand].valid){
                    hashTable ->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo);
                    if(bufTable[clockHand].dirty){
                        Status ioStatus = bufTable[clockHand].file ->writePage(bufTable[clockHand].pageNo, &bufPool[clockHand]);
                        if(ioStatus != OK){
                            return UNIXERR; // catch error
                        }
                        bufStats.diskwrites++; //write a dirty page back to disk successfully
                        // bufTable[clockHand].dirty = false; //Reset the dirty bit
                    }
                }

                frame = clockHand;
                bufTable[frame].Clear();
                bufTable[clockHand].pinCnt = 1;
                break;
            }
        }
    }while(clockHand != pointer);

    return OK;

    if (allocatedPage) {
        cout << "Allocated buffer frame: " << frame << endl;
    } else {
        cout << "Buffer pool is full. BUFFEREXCEEDED error." << endl;
    }
    return allocatedPage ? OK : BUFFEREXCEEDED;

}

/*
First check whether the page is already in the buffer pool by invoking the lookup() method on the hashtable to get a frame number.
There are two cases to be handled depending on the outcome of the lookup() call:
Case 1) Page is not in the buffer pool. Call allocBuf() to allocate a buffer frame and then call the method file->readPage() to read the page from disk into the buffer pool frame.
Next, insert the page into the hashtable. Finally, invoke Set() on the frame to set it up properly.
Set() will leave the pinCnt for the page set to 1.  Return a pointer to the frame containing the page via the page parameter.
Case 2)  Page is in the buffer pool. In this case set the appropriate refbit, increment the pinCnt for the page, and then return a pointer to the frame containing the page via the page parameter.
Returns OK if no errors occurred, UNIXERR if a Unix error occurred, BUFFEREXCEEDED if all buffer frames are pinned, HASHTBLERROR if a hash table error occurred.
*/
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{

    int frameNo;
    Status status = hashTable ->lookup(file , PageNo, frameNo);

    if(status == HASHNOTFOUND){// referenced from online external tool
        //Case 1: Page is not in the buffer pool
        //allocate a buffer frame
        status = allocBuf(frameNo);
        if(status != OK){
            return status;
        }
        //read the page from disk into the buffer pool
        status = file ->readPage(PageNo, &bufPool[frameNo]);
        if(status != OK){
            return UNIXERR;
        }
        //insert page into hashtable
        hashTable ->insert(file, PageNo, frameNo);
        //set the frame
        bufTable[frameNo].Set(file, PageNo);
    }else if(status == OK){ // Case 2: page is in the buffer pool
        bufTable[frameNo].refbit = true; //set reference bit
        bufTable[frameNo].pinCnt += 1; // increment pinCnt
    }else{
        return HASHTBLERROR;
    }

    page = &bufPool[frameNo]; //pointer to the frame containing the page

    return OK;


}

/*
Decrements the pinCnt of the frame containing (file, PageNo) and, if dirty == true, sets the dirty bit.
Returns OK if no errors occurred, HASHNOTFOUND if the page is not in the buffer pool hash table, PAGENOTPINNED if the pin count is already 0.
*/
const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    int frameNo;

    Status status = hashTable ->lookup(file , PageNo, frameNo);
    //page is not in the buffer pool
    if(status != OK){
        return HASHNOTFOUND;
    }
    //get BufDesc of frame
    BufDesc& frameDesc = bufTable[frameNo];
    //the pin count for the frame is already 0
    if(frameDesc.pinCnt <= 0){
        return PAGENOTPINNED;
    }
    //decrements the pin count of the frame
    frameDesc.pinCnt --;
    //if dirty parameter is true, set the drity bit of the frame
    if(dirty == true){
        frameDesc.dirty = true;
    }

    return OK;
}

/*
To allocate an empty page in the specified file by invoking the file->allocatePage() method.
This method will return the page number of the newly allocated page.
Then allocBuf() is called to obtain a buffer pool frame.
Next, an entry is inserted into the hash table and Set() is invoked on the frame to set it up properly.
The method returns both the page number of the newly allocated page to the caller via the pageNo parameter and a pointer to the buffer frame allocated for the page via the page parameter.
Returns OK if no errors occurred, UNIXERR if a Unix error occurred, BUFFEREXCEEDED if all buffer frames are pinned and HASHTBLERROR if a hash table error occurred. 
*/
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    //allocate an empty page into a file
    Status status = file->allocatePage(pageNo);
    if(status != OK){
        return UNIXERR;
    }
    //obtain a buffer pool frame
    int frameNo;
    status = allocBuf(frameNo);
    if(status != OK){
        if(status == BUFFEREXCEEDED){
            return BUFFEREXCEEDED;
        }else{
            return status;
        }
    }
    //insert an entry into the hash table
    status = hashTable ->insert(file, pageNo, frameNo);
    if(status != OK){
        return HASHTBLERROR;
    }
    //set the frame
    bufTable[frameNo].Set(file, pageNo);
    //the page number of the newly allocated page
    page = &bufPool[frameNo];

    return OK;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
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

/*
This method will be called by DB::closeFile() when all instances of a file have been closed (in which case all pages of the file should have been unpinned).
This method should scan bufTable for pages belonging to the file.  For each page encountered it should:
a) if the page is dirty, call file->writePage() to flush the page to disk and then set the dirty bit for the page to false
b) remove the page from the hashtable (whether the page is clean or dirty)
c) invoke the Clear() method on the page frame.
Returns OK if no errors occurred and PAGEPINNED if some page of the file is pinned.
*/
const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

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
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


