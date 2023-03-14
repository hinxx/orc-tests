#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
// htobe64
#include <endian.h>
#include <ctype.h>


#define MAX_KEY_LEN         32
#define MAX_VAL_LEN         64

void hexdump(const void *ptr, int buflen) {
  unsigned char *buf = (unsigned char*)ptr;
  int i, j;
  for (i=0; i<buflen; i+=32) {
    printf("%06x: ", i);
    for (j=0; j<32; j++)
      if (i+j < buflen)
        printf("%02x ", unsigned(buf[i+j]));
      else
        printf("   ");
    printf(" ");
    for (j=0; j<32; j++)
      if (i+j < buflen)
        printf("%c", isprint(buf[i+j]) ? buf[i+j] : '.');
    printf("\n");
  }
}

void hexdump1(const void *ptr, int buflen) {
  unsigned char *buf = (unsigned char*)ptr;
  int i;
  for (i=0; i<buflen; i++) {
    if (i%8 == 0) {
       printf(" "); 
    }
    printf("%02x", unsigned(buf[i]));
  }
}

struct data_point {
    data_point(const char *key_, const size_t key_len_, const char *value_, const size_t value_len_) {
        assert(key_len_ <= MAX_KEY_LEN);
        assert(value_len_ <= MAX_VAL_LEN);
        key_len = key_len_;
        memcpy(key, key_, key_len_);
        value_len = value_len_;
        memcpy(value, value_, value_len_);
    }

    char key[MAX_KEY_LEN] = {0};
    size_t key_len = 0;
    char value[MAX_VAL_LEN] = {0};
    size_t value_len = 0;
};


struct node_unsorted {
    // node_unsorted(const node_unsorted&) = delete;
    // node_unsorted& operator= (const node_unsorted&) = delete;
    node_unsorted(const char *key_, const size_t key_len_, const char *value_, const size_t value_len_) {
        assert(key_len_ <= MAX_KEY_LEN);
        assert(value_len_ <= MAX_VAL_LEN);
        key_len = key_len_;
        memcpy(key, key_, key_len_);
        value_len = value_len_;
        memcpy(value, value_, value_len_);
        next = NULL;
    }

    char key[MAX_KEY_LEN] = {0};
    size_t key_len = 0;
    char value[MAX_VAL_LEN] = {0};
    size_t value_len = 0;

    struct node_unsorted *next = 0;
};

struct map_unsorted {

    // map_unsorted(const map_unsorted&) = delete;
    // map_unsorted& operator= (const map_unsorted&) = delete;
    map_unsorted() {
        head = NULL;
        tail = NULL;
    }

    ~map_unsorted() {
        struct node_unsorted *tmp;
        while (head != NULL) {
            tmp = head;
            head = head->next;
            delete tmp;
        }
    }

    void insert(const char *key_, const size_t key_len_, const char *value_, const size_t value_len_) {
        if (head == NULL) {
            head = new node_unsorted(key_, key_len_, value_, value_len_);
            assert(head != NULL);
            tail = head;
        } else {
            assert(head != NULL);
            assert(tail != NULL);
            assert(tail->next == NULL);
            tail->next = new node_unsorted(key_, key_len_, value_, value_len_);
            tail = tail->next;
        }
    }

    struct node_unsorted *get(const char *key_, const size_t key_len_) {
        struct node_unsorted *tmp = head;
        while (tmp != NULL) {
            printf("%s = %s\n", tmp->key, tmp->value);
            if (key_len_ == tmp->key_len) {
                if (memcmp(key_, tmp->key, key_len_) == 0) {
                    return tmp;
                }
            }
            tmp = tmp->next;
        }
        return NULL;
    }

    struct node_unsorted *head = NULL;
    struct node_unsorted *tail = NULL;
};


struct node_sorted {
    // node_sorted(const node_sorted&) = delete;
    // node_sorted& operator= (const node_sorted&) = delete;
    node_sorted(const char *key_, const size_t key_len_, const size_t value_) {
        assert(key_len_ <= MAX_KEY_LEN);
        key_len = key_len_;
        memcpy(key, key_, key_len_);
        value = value_;
        left = NULL;
        right = NULL;
    }

    char key[MAX_KEY_LEN] = {0};
    size_t key_len = 0;
    size_t value = 0;

    struct node_sorted *left = NULL;
    struct node_sorted *right = NULL;
};

// see https://www.codesdope.com/blog/article/binary-search-tree-in-c/
struct map_sorted {

    // map_sorted(const map_sorted&) = delete;
    // map_sorted& operator= (const map_sorted&) = delete;
    map_sorted() {
        root = NULL;
    }

    ~map_sorted() {
        destroy(root);
    }

    void destroy(struct node_sorted *node_) {
        if (node_ == NULL) {
            return;
        }
        destroy(node_->left);
        node_->left = NULL;
        printf("%s = %ld\n", node_->key, node_->value);
        destroy(node_->right);
        node_->right = NULL;
        delete node_;
    }

    void insert(const char *key_, const size_t key_len_, const size_t value_) {
        root = insert(root, key_, key_len_, value_);
    }

    struct node_sorted *insert(struct node_sorted *node_, const char *key_, const size_t key_len_, const size_t value_) {
        if (node_ == NULL) {
            return new node_sorted(key_, key_len_, value_);
        } else if (node_->value <= value_) {
            node_->right = insert(node_->right, key_, key_len_, value_);
        } else {
            node_->left = insert(node_->left, key_, key_len_, value_);
        }
        return node_;
    }

    struct node_sorted *get(const char *key_, const size_t key_len_) {
        return get(root, key_, key_len_);
    }

    struct node_sorted *get(struct node_sorted *node_, const char *key_, const size_t key_len_) {
        if (node_ == NULL) {
            return NULL;
        }
        printf("%s = %ld\n", node_->key, node_->value);
        if (key_len_ == node_->key_len) {
            int ret = memcmp(key_, node_->key, key_len_);
            if (ret == 0) {
                // match
                return node_;
            } else if (ret < 0) {
                return get(node_->left, key_, key_len_);
            } else if (ret > 0) {
                return get(node_->right, key_, key_len_);
            }
        } else if (key_len_ < node_->key_len) {
            int ret = memcmp(key_, node_->key, key_len_);
            if (ret <= 0) {
                return get(node_->left, key_, key_len_);
            } else if (ret > 0) {
                return get(node_->right, key_, key_len_);
            }
        } else if (key_len_ > node_->key_len) {
            int ret = memcmp(key_, node_->key, node_->key_len);
            if (ret <= 0) {
                return get(node_->right, key_, key_len_);
            } else if (ret > 0) {
                return get(node_->left, key_, key_len_);
            }
        }

        // should not get here?!
        assert(node_ == NULL);
        return NULL;
    }

    struct node_sorted *del(const char *key_, const size_t key_len_) {
        root = del(root, key_, key_len_);
        return root;
    }

    struct node_sorted *minimum(struct node_sorted *node_) {
        if (node_ == NULL) {
            return NULL;
        } else if (node_->left != NULL) {
            // node with minimum value will have no left child
            return minimum(node_->left);
        }
        return node_;
    }

    struct node_sorted *del(struct node_sorted *node_, const char *key_, const size_t key_len_) {
        if (node_ == NULL) {
            return NULL;
        }
        printf("%s = %ld\n", node_->key, node_->value);
        if (key_len_ == node_->key_len) {
            int ret = memcmp(key_, node_->key, key_len_);
            if (ret == 0) {
                // match
                if (node_->left == NULL && node_->right == NULL) {
                    // no children
                    delete node_;
                    return NULL;
                } else if (node_->left == NULL || node_->right == NULL) {
                    // one child
                    struct node_sorted *tmp;
                    if (node_->left == NULL) {
                        tmp = node_->right;
                    } else {
                        tmp = node_->left;
                    }
                    delete node_;
                    return tmp;
                } else {
                    // two children
                    struct node_sorted *tmp = minimum(node_->right);
                    memcpy(node_->key, tmp->key, tmp->key_len);
                    node_->key_len = tmp->key_len;
                    node_->value = tmp->value;
                    node_->right = del(node_->right, tmp->key, tmp->key_len);
                }
            } else if (ret < 0) {
                node_->left = del(node_->left, key_, key_len_);
            } else if (ret > 0) {
                node_->right = del(node_->right, key_, key_len_);
            }
        } else if (key_len_ < node_->key_len) {
            int ret = memcmp(key_, node_->key, key_len_);
            if (ret <= 0) {
                node_->left = del(node_->left, key_, key_len_);
            } else if (ret > 0) {
                node_->right = del(node_->right, key_, key_len_);
            }
        } else if (key_len_ > node_->key_len) {
            int ret = memcmp(key_, node_->key, node_->key_len);
            if (ret <= 0) {
                node_->right = del(node_->right, key_, key_len_);
            } else if (ret > 0) {
                node_->left = del(node_->left, key_, key_len_);
            }
        }

        return node_;
    }


    void inorder() {
        inorder(root);
    }

    void inorder(struct node_sorted *node_) {
        if (node_ != NULL) {
            inorder(node_->left);
            printf("%s = %ld\n", node_->key, node_->value);
            inorder(node_->right);
        }
    }

    struct node_sorted *root = NULL;
};


void test1() {
    struct map_unsorted *umap = new struct map_unsorted();
    umap->insert("a1", 2, "1111", 4);
    umap->insert("b2", 2, "2222", 4);
    umap->insert("c3", 2, "3333", 4);
    umap->insert("e5", 2, "5555", 4);

    {
        struct node_unsorted *n = umap->get("ii", 2);
        if (n == NULL) {
            printf("not found!\n");
        } else {
            printf("found: %s = %s\n", n->key, n->value);
        }
    }
    {
        struct node_unsorted *n = umap->get("c3", 2);
        if (n == NULL) {
            printf("not found!\n");
        } else {
            printf("found: %s = %s\n", n->key, n->value);
        }
    }
    delete umap;
}

void test2() {
    struct map_sorted *smap = new struct map_sorted();
    smap->insert("a1", 2, 11);
    smap->insert("b2", 2, 22);
    smap->insert("d4", 2, 44);
    smap->insert("c3", 2, 33);
    smap->insert("a0", 2, 00);

    printf("INORDER\n");
    smap->inorder();
    
    {
        printf("GET\n");
        node_sorted *n = smap->get("x9", 2);
        if (n == NULL) {
            printf("not found!\n");
        } else {
            printf("found: %s = %ld\n", n->key, n->value);
        }
    }

    {
        printf("GET\n");
        node_sorted *n = smap->get("d4", 2);
        if (n == NULL) {
            printf("not found!\n");
        } else {
            printf("found: %s = %ld\n", n->key, n->value);
        }
    }

    printf("DEL\n");
    smap->del("d4", 2);
    printf("INORDER\n");
    smap->inorder();

    printf("DESTROY\n");
    delete smap;
}

void test3() {
    struct map_sorted *smap = new struct map_sorted();
    smap->insert("a20", 3, 20);
    smap->insert("a05", 3, 5);
    smap->insert("a01", 3, 1);
    smap->insert("a15", 3, 15);
    smap->insert("a09", 3, 9);
    smap->insert("a07", 3, 7);
    smap->insert("a12", 3, 12);
    smap->insert("a30", 3, 30);
    smap->insert("a25", 3, 25);
    smap->insert("a40", 3, 40);
    smap->insert("a45", 3, 45);
    smap->insert("a42", 3, 42);

    printf("INORDER\n");
    smap->inorder();

    printf("DEL a01\n");
    smap->del("a01", 3);
    printf("INORDER\n");
    smap->inorder();

    printf("DEL a40\n");
    smap->del("a40", 3);
    printf("INORDER\n");
    smap->inorder();

    printf("DEL a45\n");
    smap->del("a45", 3);
    printf("INORDER\n");
    smap->inorder();

    printf("DEL a09\n");
    smap->del("a09", 3);
    printf("INORDER\n");
    smap->inorder();

    printf("DESTROY\n");
    delete smap;
}

struct data_point *make_data_point(u_int64_t ts, const char *name) {
    char key[MAX_KEY_LEN] = {0};
    size_t key_len = 0;
    char value[MAX_VAL_LEN] = {0};
    size_t value_len = 0;

    // create key
    *(u_int64_t *)key = htobe64(ts);
    key_len += sizeof(u_int64_t);
    memcpy(key + key_len, name, strlen(name));
    key_len += strlen(name);
    printf("key len = %ld\n", key_len);
    printf("key     = %ld%s\n", ts, name);

    // create random length value
    while (value_len < 10) {
        value_len = rand() % MAX_VAL_LEN;
    }
    printf("value len = %ld\n", value_len);
    for (size_t i = 0; i < value_len; i++) {
        value[i] = (u_int8_t)i;
    }

    hexdump1(key, key_len);
    printf(" =");
    hexdump1(value, value_len);
    printf("\n");

    struct data_point *dp = new struct data_point(key, key_len, value, value_len);
    return dp;
}

void test4() {
    srand(9999);

    struct map_sorted *smap = new struct map_sorted();
    struct map_unsorted *umap = new struct map_unsorted();

    {
        struct data_point *dp = make_data_point(10, "foobar");
        smap->insert(dp->key, dp->key_len, dp->value_len);
        umap->insert(dp->key, dp->key_len, dp->value, dp->value_len);
        delete dp;
    }

    // make_data_point(12, "zanzibar");
    // make_data_point(17, "pineapple");
    // make_data_point(10, "grapefruit");
    // make_data_point(11, "lemon");

    printf("INORDER\n");
    smap->inorder();

    // printf("DEL a01\n");
    // smap->del("a01", 3);
    // printf("INORDER\n");
    // smap->inorder();

    // printf("DEL a40\n");
    // smap->del("a40", 3);
    // printf("INORDER\n");
    // smap->inorder();

    // printf("DEL a45\n");
    // smap->del("a45", 3);
    // printf("INORDER\n");
    // smap->inorder();

    // printf("DEL a09\n");
    // smap->del("a09", 3);
    // printf("INORDER\n");
    // smap->inorder();

    printf("DESTROY\n");
    delete smap;
}

int main(int argc, char const *argv[]) {

    // test1();
    // test2();
    // test3();
    test4();


    return 0;
}
