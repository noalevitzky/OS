#include "VirtualMemory.h"
#include "PhysicalMemory.h"
#include <cmath>

void clearTable(uint64_t frameIndex) {
    for (uint64_t i = 0; i < PAGE_SIZE; ++i) {
        PMwrite(frameIndex * PAGE_SIZE + i, 0);
    }
}

/**
 * will be called before any other function is called.
 */
void VMinitialize() {
    clearTable(0);
}
/**
 * // Function to extract k bits from p position
// and returns the extracted value as integer
 * @param number
 * @param k
 * @param p
 * @return
 */
uint64_t bitExtracted(uint64_t number, int k, int p)
{
    return (((1 << k) - 1) & (number >> p));
}


struct dfsFindings {
    int emptyFrameParentIndex = 0;
    int emptyFrameParentOffset = 0;
    int emptyFrameIndex = 0;
    int maxFramesUsed = 0;
    int heaviestPageWeight = 0;
    int heaviestPageParent = 0;
    int heaviestPageFrameIndex = 0;
    int heaviestPageParentOffset = 0;
    uint64_t virtualAddress = 0;
};

void dfs(int depth, int frameIndex, int parentIndex, int parentOffset, dfsFindings& dfsFindings, uint64_t virtualAddress,
        int weight, int indexToBeSaved)
{
    // Reached a page
    if (TABLES_DEPTH == depth)
    {
        // save details in order to evict later
       if (weight > dfsFindings.heaviestPageWeight ||
       (weight == dfsFindings.heaviestPageWeight && frameIndex < dfsFindings.heaviestPageFrameIndex))
       {
           dfsFindings.heaviestPageWeight = weight;
           dfsFindings.heaviestPageParent = parentIndex;
           dfsFindings.heaviestPageFrameIndex = frameIndex;
           dfsFindings.heaviestPageParentOffset = parentOffset;
           dfsFindings.virtualAddress = virtualAddress;
       }

       return;
    }

    // Reached a page table
    bool pageTableEmpty = true;
    word_t nextFrame = -1;
    int pageSize = PAGE_SIZE;
    if (depth == 0){
        int rootPageTableWidth = (VIRTUAL_ADDRESS_WIDTH - OFFSET_WIDTH) % OFFSET_WIDTH;
        pageSize = (rootPageTableWidth == 0) ? PAGE_SIZE : pow(2, rootPageTableWidth);
    }
    for (int i = 0 ; i < pageSize ; i++)
    {
        PMread((frameIndex * PAGE_SIZE) + i, &nextFrame);
        if (nextFrame!=0)
        {
            if (nextFrame > dfsFindings.maxFramesUsed)
            {
                dfsFindings.maxFramesUsed = nextFrame;
            }
            pageTableEmpty = false;
            int weightAddition = (nextFrame % 2 == 0) ? WEIGHT_EVEN: WEIGHT_ODD;
            uint64_t newAddress = ((virtualAddress << OFFSET_WIDTH) | (uint64_t) i);
            dfs(depth+1, nextFrame, frameIndex, i, dfsFindings,
                newAddress, weight + weightAddition,
                indexToBeSaved);
        }
    }

    // reached an empty table that is not part of the path we are creating
    // Save in order to clear reference later
    if (pageTableEmpty && frameIndex != indexToBeSaved)
    {
        dfsFindings.emptyFrameParentIndex = parentIndex;
        dfsFindings.emptyFrameIndex = frameIndex;
        dfsFindings.emptyFrameParentOffset = parentOffset;
    }

    return;
}

int findNextUnusedFrame(int indexToSave, uint64_t virtualAddress){
    dfsFindings result;
    dfs(0, 0, 0, 0, result, (uint64_t) 0, WEIGHT_EVEN, indexToSave);

    // Reached an empty page table
    if (result.emptyFrameIndex != 0)
    {
        // Remove the reference to this table from it's parent
        PMwrite(result.emptyFrameParentIndex * PAGE_SIZE + result.emptyFrameParentOffset, 0);
        return result.emptyFrameIndex;
    }

    // Else, reached a free frame in RAM
    if (result.maxFramesUsed + 1 < NUM_FRAMES)
    {
        return result.maxFramesUsed + 1;
    }

    // Else, memory is in full use. Evict a frame (choose by weight)
    PMevict(result.heaviestPageFrameIndex, result.virtualAddress);
    // Remove the reference to this table from it's parent
    PMwrite(result.heaviestPageParent * PAGE_SIZE + result.heaviestPageParentOffset, 0);
    return result.heaviestPageFrameIndex;
}

// 01101

uint64_t convertVirtualAddress(uint64_t virtualAddress)
{
    int pageOffset = bitExtracted(virtualAddress, OFFSET_WIDTH, 0);
    int position;
    bool pathToPageExist = true;
    word_t frameBegin = 0;
    word_t parent;
    uint64_t physicalAddress;
    int rootPageTableWidth = (VIRTUAL_ADDRESS_WIDTH - OFFSET_WIDTH) % OFFSET_WIDTH;
    if (rootPageTableWidth == 0)
    {
        rootPageTableWidth = OFFSET_WIDTH;
    }
    int pageBitsLength = -1;
    int frameOffset = -1;
    for (int i = 0 ; i < TABLES_DEPTH ; i++)
    {
        position = OFFSET_WIDTH * (TABLES_DEPTH - i);
        if (i == 0)
        {
            pageBitsLength = rootPageTableWidth;
        }
        else
        {
            pageBitsLength = OFFSET_WIDTH;
        }
        frameOffset = ((1 << pageBitsLength) - 1) & (virtualAddress >> position);
        parent = frameBegin;
        PMread((frameBegin * PAGE_SIZE) + frameOffset, &frameBegin);
        if (frameBegin == 0)
        {
            pathToPageExist = false;
            int unusedFrame = findNextUnusedFrame(parent, virtualAddress);
            if (i < TABLES_DEPTH -1) // todo why?
            {
                clearTable(unusedFrame);
            }
            PMwrite((parent * PAGE_SIZE) + frameOffset, unusedFrame);
            frameBegin = unusedFrame;
        }
    }
    if (!pathToPageExist) // if path does not exit, we need to restore page to memory
    {
        PMrestore(frameBegin, virtualAddress >> OFFSET_WIDTH);
    }

    physicalAddress = (frameBegin * PAGE_SIZE) + pageOffset;
    return physicalAddress;
}

/**
 * reads the word from the virtual address virtualAddress into *value. Returns 1 on success and 0 on failure.
 * @param virtualAddress
 * @param value
 * @return
 */
int VMread(uint64_t virtualAddress, word_t* value) {

    if (virtualAddress < VIRTUAL_MEMORY_SIZE)
    {
        uint64_t physicalAddress = convertVirtualAddress(virtualAddress);
        PMread(physicalAddress, value);
        return 1;
    }
    return 0;
}

/**
 * writes the word value into the virtual address virtualAddress. Returns 1 on success and 0 on failure.
 * @param virtualAddress
 * @param value
 * @return
 */
int VMwrite(uint64_t virtualAddress, word_t value) {

    if (virtualAddress < VIRTUAL_MEMORY_SIZE)
    {
        uint64_t physicalAddress = convertVirtualAddress(virtualAddress);
        PMwrite(physicalAddress, value);
        return 1;
    }
    return 0;
}
