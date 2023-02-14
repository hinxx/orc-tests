

#include <orc/ColumnPrinter.hh>
#include <orc/OrcFile.hh>


std::unique_ptr<orc::Writer> myCreateWriter(
                                      uint64_t stripeSize,
                                      uint64_t compresionblockSize,
                                      orc::CompressionKind compression,
                                      const orc::Type& type,
                                      orc::MemoryPool* memoryPool,
                                      orc::OutputStream* stream,
                                      orc::FileVersion version,
                                      uint64_t stride = 0,
                                      const std::string& timezone = "GMT"){
    orc::WriterOptions options;
    options.setStripeSize(stripeSize);
    options.setCompressionBlockSize(compresionblockSize);
    options.setCompression(compression);
    options.setMemoryPool(memoryPool);
    options.setRowIndexStride(stride);
    options.setFileVersion(version);
    options.setTimezoneName(timezone);
    return createWriter(type, stream, options);
  }

void test_empty() {
    std::unique_ptr<orc::OutputStream> outFile = orc::writeLocalFile("file-empty.orc");
    orc::MemoryPool *pool = orc::getDefaultPool();
    ORC_UNIQUE_PTR<orc::Type> type(orc::Type::buildTypeFromString("struct<col1:int>"));

    uint64_t stripeSize = 16 * 1024; // 16K
    uint64_t compressionBlockSize = 1024; // 1k

    std::unique_ptr<orc::Writer> writer = myCreateWriter(
                                      stripeSize,
                                      compressionBlockSize,
                                      orc::CompressionKind_NONE,
                                      *type,
                                      pool,
                                      outFile.get(),
                                      orc::FileVersion::v_0_11());

    writer->close();

}

void test_one_stripe() {
    std::unique_ptr<orc::OutputStream> outFile = orc::writeLocalFile("file-one-stripe.orc");
    orc::MemoryPool *pool = orc::getDefaultPool();
    ORC_UNIQUE_PTR<orc::Type> type(orc::Type::buildTypeFromString("struct<col1:int>"));

    uint64_t stripeSize = 16 * 1024; // 16K
    uint64_t compressionBlockSize = 1024; // 1k

    std::unique_ptr<orc::Writer> writer = myCreateWriter(
                                      stripeSize,
                                      compressionBlockSize,
                                      orc::CompressionKind_NONE,
                                      *type,
                                      pool,
                                      outFile.get(),
                                      orc::FileVersion::v_0_11());
    
    // file with one stripe
    std::unique_ptr<orc::ColumnVectorBatch> batch = writer->createRowBatch(1024);
    orc::StructVectorBatch* structBatch = dynamic_cast<orc::StructVectorBatch *>(batch.get());
    orc::LongVectorBatch* longBatch = dynamic_cast<orc::LongVectorBatch *>(structBatch->fields[0]);

    for (uint64_t i = 0; i < 1024; ++i) {
        longBatch->data[i] = static_cast<int64_t>(i);
    }
    structBatch->numElements = 1024;
    longBatch->numElements = 1024;

    writer->add(*batch);

    // for (uint64_t i = 1024; i < 2000; ++i) {
    //   longBatch->data[i - 1024] = static_cast<int64_t>(i);
    // }
    // structBatch->numElements = 2000 - 1024;
    // longBatch->numElements = 2000 - 1024;

    // writer->add(*batch);
    // writer->addUserMetadata("name0","value0");
    // writer->addUserMetadata("name1","value1");

    writer->close();

}

void test_multi_stripe(unsigned rowBatchSize) {
    std::unique_ptr<orc::OutputStream> outFile = orc::writeLocalFile("file-multi-stripe.orc");
    orc::MemoryPool *pool = orc::getDefaultPool();
    ORC_UNIQUE_PTR<orc::Type> type(orc::Type::buildTypeFromString("struct<col1:int>"));

    uint64_t stripeSize = 1024; // 1K
    uint64_t compressionBlockSize = 1024; // 1k

    std::unique_ptr<orc::Writer> writer = myCreateWriter(
                                      stripeSize,
                                      compressionBlockSize,
                                      orc::CompressionKind_NONE,
                                      *type,
                                      pool,
                                      outFile.get(),
                                      orc::FileVersion::v_0_11());

    // multiple stripes
    std::unique_ptr<orc::ColumnVectorBatch> batch = writer->createRowBatch(rowBatchSize);
    orc::StructVectorBatch* structBatch = dynamic_cast<orc::StructVectorBatch*>(batch.get());
    orc::LongVectorBatch* longBatch = dynamic_cast<orc::LongVectorBatch*>(structBatch->fields[0]);

    // generate >1 stripes since the batch size is larger than stripe size
    // we set the minimum strip size and not the exact stripe size
    for (uint64_t j = 0; j < 10; ++j) {
        for (uint64_t i = 0; i < rowBatchSize; ++i) {
            longBatch->data[i] = static_cast<int64_t>(i);
        }
        structBatch->numElements = rowBatchSize;
        longBatch->numElements = rowBatchSize;

        writer->add(*batch);
    }

    writer->close();
}

int main(int argc, char const *argv[]) {
    test_empty();
    test_one_stripe();
    
    // 10 stripes
    // test_multi_stripe(65*1024);
    // test_multi_stripe(1024);
    // test_multi_stripe(512);
    
    // only 5 stripes
    test_multi_stripe(10);
    return 0;
}

