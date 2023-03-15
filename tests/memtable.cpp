#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <endian.h>
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

    void hexdump() {
        for (size_t i = 0; i < size_; i++) {
            if (i % 8 == 0) {
                printf(" ");
            }
            printf("%02x", unsigned(data_[i]));
        }
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
    DataPoint(struct Arena *arena) : left_(nullptr), right_(nullptr), arena_(arena) {}

    // key is ts + name
    void set(const char *key, const size_t key_len, const char *value, const size_t value_len) {
        char *ptr = (char *)this + sizeof(*this);
        key_ = Slice(ptr, key_len);
        memcpy(ptr, key, key_len);
        ptr += key_len;
        value_ = Slice(ptr, value_len);
        memcpy(ptr, value, value_len);
    }

    void set(const u_int64_t ts, const char *name, const size_t name_len, const char *value, const size_t value_len) {
        char *ptr = (char *)this + sizeof(*this);
        key_ = Slice(ptr, ts_size_ + name_len);
        u_int64_t bets = htobe64(ts);
        memcpy(ptr, &bets, ts_size_);
        ptr += ts_size_;
        memcpy(ptr, name, name_len);
        ptr += name_len;
        value_ = Slice(ptr, value_len);
        memcpy(ptr, value, value_len);
    }

    void toString() {
        Slice ts = Slice(key_.data_, ts_size_);
        Slice name = Slice(key_.data_ + ts_size_, key_.size_ - ts_size_);
        printf("%08ld '%.*s'='%.*s'\n", htobe64(*(u_int64_t *)ts.data_), (int)name.size_, name.data_, (int)value_.size_, value_.data_);
    }

    static constexpr u_int64_t ts_size_ = sizeof(u_int64_t);
    struct Slice key_;
    struct Slice value_;
    struct DataPoint *left_;
    struct DataPoint *right_;
    // is this needed?
    struct Arena *arena_;
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

    ~Arena() {
        struct Block *head, *tmp;
        head = head_.next_;
        while (head) {
            tmp = head->next_;
            free(head);
            head = tmp;
        }
    }

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
        printf("arena block %p free space %ld\n", current_, freeSpace());

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
                printf("created arena block %p free space %ld\n", block, freeSpace(block));
            }
        }

        block->avail_ += size;
        return block->avail_ - size;
    }

    size_t freeSpace(struct Block *block = nullptr) {
        if (block != nullptr) {
            return (block->limit_ - block->avail_);
        } else {
            return (current_->limit_ - current_->avail_);
        }
    }

    struct Block head_;
    struct Block *current_;
};

struct Memtable {
    Memtable() : root_(nullptr) {}

    struct DataPoint *allocateDataPoint(const size_t size) {
        char *ptr = (char *)arena_.allocate(size + sizeof(struct DataPoint));
        struct DataPoint *dp = new (ptr) DataPoint(&arena_);
        return dp;
    }

    void insert(const u_int64_t ts, const char *name, const size_t name_len, const char *value, const size_t value_len) {
        size_t size = DataPoint::ts_size_ + name_len + value_len;
        struct DataPoint *dp = allocateDataPoint(size);
        dp->set(ts, name, name_len, value, value_len);
        dp->toString();
        insert(dp);
    }

    void insert(struct DataPoint *dp) {
        root_ = insert(root_, dp);
    }

    struct DataPoint *insert(struct DataPoint *node, struct DataPoint *dp) {
        if (node == nullptr) {
            return dp;
        } else if (node->key_.compare(dp->key_) <= 0) {
            node->right_ = insert(node->right_, dp);
        } else {
            node->left_ = insert(node->left_, dp);
        }
        return node;
    }

    void inorder() {
        inorder(root_);
    }

    void inorder(struct DataPoint *node) {
        if (node != nullptr) {
            inorder(node->left_);
            node->toString();
            inorder(node->right_);
        }
    }

    struct DataPoint *root_;
    struct Arena arena_;
};








int main(int argc, char const *argv[]) {

    struct Memtable mt;
    u_int64_t ts = 1;

    // overallocated size
    struct DataPoint *dp = mt.allocateDataPoint(100);
    // manual initialization of data point
    dp->set(ts, (const char *)"test", 4, (const char *)"manual", 6);
    dp->toString();
    // insertion in to binary tree
    mt.insert(dp);

    // all of the above in one call
    mt.insert(ts, (const char *)"name", 4, (const char *)"value", 5);
    mt.insert(ts + 1, (const char *)"blahblah", 8, (const char *)"with space value", 16);
    mt.insert(ts, (const char *)"aaaaaa", 6, (const char *)"muchbetter", 10);
    printf("IN ORDER:\n");
    mt.inorder();

    return 0;
}
