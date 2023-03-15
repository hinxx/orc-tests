#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <new>


struct Slice {
    // Create an empty slice.
    Slice() : data_(""), size_(0) {}
    
    // Create a slice that refers to d[0,n-1].
    Slice(const char* d, size_t n) : data_(d), size_(n) {}
    
    // Create a slice that refers to s[0,strlen(s)-1], implicit
    Slice(const char* s) : data_(s) { size_ = (s == nullptr) ? 0 : strlen(s); }

    // Return true iff the length of the referenced data is zero
    bool empty() const { return size_ == 0; }
    
    // Return the ith byte in the referenced data.
    char operator[](size_t n) const {
        assert(n < size_);
        return data_[n];
    }

    // Change this slice to refer to an empty array
    void clear() {
        data_ = "";
        size_ = 0;
    }

    // Three-way comparison.  Returns value:
    //   <  0 iff "*this" <  "b",
    //   == 0 iff "*this" == "b",
    //   >  0 iff "*this" >  "b"
    int compare(const Slice &b) const;

    const char* data_;
    size_t size_;
};

inline int Slice::compare(const Slice &b) const {
    assert(data_ != nullptr && b.data_ != nullptr);
    const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
    int r = memcmp(data_, b.data_, min_len);
    if (r == 0) {
        if (size_ < b.size_)
            r = -1;
        else if (size_ > b.size_)
            r = +1;
    }
    return r;
}

inline bool operator==(const Slice &x, const Slice &y) {
    return ((x.size_ == y.size_) && (memcmp(x.data_, y.data_, x.size_) == 0));
}

inline bool operator!=(const Slice& x, const Slice& y) {
    return !(x == y);
}


struct DataPoint {
    DataPoint(struct Arena *arena) : left_(nullptr), right_(nullptr), arena_(arena),
                                     ptr_key_(nullptr), ptr_value_(nullptr) {
        // ptr_key_ = (char *)this + sizeof(*this);
        // ptr_value_ = (char *)this + sizeof(*this);
    }

    // void setKey(const char *key, const size_t len) {
    //     memcpy(ptr_key_, key, len);
    // }
    // void setValue(const char *value, const size_t len) {
    //     memcpy(ptr_value_, value, len);
    // }

    void set(const char *key, const size_t key_len, const char *value, const size_t value_len) {
        ptr_key_ = (char *)this + sizeof(*this);
        memcpy(ptr_key_, key, key_len);
        ptr_value_ = (char *)this + sizeof(*this) + key_len;
        memcpy(ptr_value_, value, value_len);
        key_ = Slice(ptr_key_, key_len);
        value_ = Slice(ptr_value_, value_len);
    }

    struct Slice key_;
    struct Slice value_;
    struct DataPoint *left_;
    struct DataPoint *right_;
    // is this needed?
    struct Arena *arena_;
    char *ptr_key_;
    char *ptr_value_;
};

struct Block {
    // Empty block (head)
    Block() : next_(nullptr), limit_(nullptr), avail_(nullptr) {
        limit_ = (char *)this + sizeof(*this);
        avail_ = limit_;
    }
    ~Block() {}
    
    struct Block *next_;
    char *limit_;
    char *avail_;
};

struct Arena {
    Arena() : head_(Block()), current_(&head_) {}

    char *allocate(const size_t size) { 
        char *ptr = NULL;
        if ((current_->avail_ + size) > current_->limit_) {
            // not enough space in the current block, find/allocate a new one
            ptr = findOrCreate(size, &current_);
        } else {
            // current block has enough space
            ptr = current_->avail_;
            // move the avail pointer
            current_->avail_ += size;
        }
        return ptr;
    }

    void deallocate() {
        if (head_.next_) {
            current_ = head_.next_;
            current_->avail_ = (char *)current_ + sizeof(*current_);
        } else {
            current_ = &head_;
        }
    }

    char *findOrCreate(const size_t size, struct Block **ptr_block) {
        struct Block *block = nullptr;

        // iterate while there is *not* enough space found in the blocks
        for (block = *ptr_block; (block->avail_ + size) > block->limit_; *ptr_block = block) {
            if (block->next_) {
                // try next block
                block = block->next_;
                block->avail_ = (char *)block + sizeof(*block);
            } else {
                // allocate a block and use placement new to initialize Block
                int sz = 4 * 1024;
                char *ptr = (char *)calloc(1, sz);
                block->next_ = new (ptr) Block();
                block = block->next_;
                block->limit_ = (char *)block + sz;
                block->avail_ = (char *)block + sizeof(*block);
                block->next_ = NULL;
            }
        }

        block->avail_ += size;
        return block->avail_ - size;
    }

    struct Block head_;
    struct Block *current_;
};

struct Memtable {
    Memtable() : root_(nullptr) {}

    struct DataPoint *allocateDataPoint(const size_t size) {
        // struct DataPoint *dp = (struct DataPoint *)arena_.allocate(size + sizeof(struct DataPoint));
        char *ptr = (char *)arena_.allocate(size + sizeof(struct DataPoint));
        struct DataPoint *dp = new (ptr) DataPoint(&arena_);
        // dp->arena_ = &arena_;
        return dp;
    }

    struct DataPoint *root_;
    struct Arena arena_;
};








int main(int argc, char const *argv[]) {

    struct Memtable mt;
    struct DataPoint *dp = mt.allocateDataPoint(100);
    printf("dp %p\n", dp);

    return 0;
}
