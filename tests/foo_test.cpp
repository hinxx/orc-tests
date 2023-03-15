#include "gtest/gtest.h"

class Func1 {
public:

    Func1(int x) {
        x_ = x;
    }

    int add(int val) {
        return val + x_;
    }

    int sub(int val) {
        return val - x_;
    }

private:
    int x_;
};

class Func1Test : public testing::Test {
protected:
    void SetUp() override {
        // printf("%s: ..\n", __func__);
        func1 = new Func1(9);
    }
    void TearDown() override {
        // printf("%s: ..\n", __func__);
        delete func1;
    }

    Func1 *func1;
};

TEST_F(Func1Test, Add1) {
    ASSERT_EQ(func1->add(1), 10);
}

TEST_F(Func1Test, Add2) {
    ASSERT_EQ(func1->add(0), 9);
}

TEST_F(Func1Test, Add3) {
    ASSERT_EQ(func1->add(-1), 8);
}


int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
