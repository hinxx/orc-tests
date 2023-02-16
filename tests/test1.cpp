#include <stdio.h>
#include <time.h>

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
                                      const std::string& timezone = "GMT") {
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


std::unique_ptr<orc::Reader> myCreateReader(
                                      orc::MemoryPool * memoryPool,
                                      std::unique_ptr<orc::InputStream> stream) {
    orc::ReaderOptions options;
    options.setMemoryPool(*memoryPool);
    return createReader(std::move(stream), options);
}

std::unique_ptr<orc::RowReader> myCreateRowReader(
                                            orc::Reader* reader,
                                            const std::string& timezone = "GMT") {
    orc::RowReaderOptions rowReaderOpts;
    rowReaderOpts.setTimezoneName(timezone);
    return reader->createRowReader(rowReaderOpts);
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

    // printf("longBatch name %s\n", longBatch->toString().c_str());
    printf("longBatch memory usage %ld\n", longBatch->getMemoryUsage());
    writer->add(*batch);
    // printf("longBatch name %s\n", longBatch->toString().c_str());
    printf("longBatch memory usage %ld\n", longBatch->getMemoryUsage());

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

    // printf("batch memory usage %ld\n", batch->getMemoryUsage());
    // printf("batch capacity %ld\n", batch->capacity);
    // printf("batch numElements %ld\n", batch->numElements);

    // generate >1 stripes since the batch size is larger than stripe size
    // we set the minimum strip size and not the exact stripe size
    for (uint64_t j = 0; j < 10; ++j) {
        for (uint64_t i = 0; i < rowBatchSize; ++i) {
            longBatch->data[i] = static_cast<int64_t>(i);
        }
        structBatch->numElements = rowBatchSize;
        longBatch->numElements = rowBatchSize;

        // printf("structBatch memory usage %ld\n", structBatch->getMemoryUsage());
        // printf("longBatch memory usage %ld\n", longBatch->getMemoryUsage());
        // printf("batch capacity %ld\n", batch->capacity);
        // printf("batch numElements %ld\n", batch->numElements);
        writer->add(*batch);
    }

    writer->close();
}

void test_array() {
    std::unique_ptr<orc::OutputStream> outFile = orc::writeLocalFile("file-array.orc");
    orc::MemoryPool *pool = orc::getDefaultPool();
    ORC_UNIQUE_PTR<orc::Type> type(orc::Type::buildTypeFromString("struct<col1:array<int>>"));

    uint64_t stripeSize = 1024 * 1024;
    uint64_t compressionBlockSize = 64 * 1024;
    // uint64_t rowCount = 1024;
    uint64_t rowCount = 20;
    uint64_t maxListLength = 10;
    uint64_t offset = 0;

    std::unique_ptr<orc::Writer> writer = myCreateWriter(
                                      stripeSize,
                                      compressionBlockSize,
                                      orc::CompressionKind_NONE,
                                      *type,
                                      pool,
                                      outFile.get(),
                                      orc::FileVersion::v_0_11());

    std::unique_ptr<orc::ColumnVectorBatch> batch = writer->createRowBatch(rowCount * maxListLength);

    orc::StructVectorBatch *structBatch = dynamic_cast<orc::StructVectorBatch *>(batch.get());
    orc::ListVectorBatch *listBatch = dynamic_cast<orc::ListVectorBatch *>(structBatch->fields[0]);
    orc::LongVectorBatch *intBatch = dynamic_cast<orc::LongVectorBatch *>(listBatch->elements.get());

    // data is in LongVectorBatch
    int64_t * data = intBatch->data.data();
    // offsets are in ListVectorBatch
    int64_t * offsets = listBatch->offsets.data();

    for (uint64_t i = 0; i < rowCount; ++i) {
        offsets[i] = static_cast<int64_t>(offset);
        printf("offsets[%3ld] = %ld\n", i, offsets[i]);
        for (uint64_t length = i % maxListLength + 1; length != 0; --length) {
            data[offset++] = static_cast<int64_t >(i);
        }
    }
    offsets[rowCount] = static_cast<int64_t>(offset);
    printf("offsets[%3ld] = %ld\n", rowCount, offsets[rowCount]);

    structBatch->numElements = rowCount;
    listBatch->numElements = rowCount;
    // Q: Why is intBatch->numElements not set?

    writer->add(*batch);
    writer->close();

    // read the data back
    std::unique_ptr<orc::InputStream> inFile = orc::readLocalFile("file-array.orc");
    std::unique_ptr<orc::Reader> reader = myCreateReader(pool, std::move(inFile));
    std::unique_ptr<orc::RowReader> rowReader = myCreateRowReader(reader.get());
    printf("rowCount == reader->getNumberOfRows(): %ld == %ld\n", rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount * maxListLength);
    printf("true == rowReader->next(*batch): %d\n", rowReader->next(*batch));

    structBatch = dynamic_cast<orc::StructVectorBatch *>(batch.get());
    listBatch = dynamic_cast<orc::ListVectorBatch *>(structBatch->fields[0]);
    intBatch = dynamic_cast<orc::LongVectorBatch *>(listBatch->elements.get());

    // listBatch/intBatch numElements were set in ColumnReader::next() called from rowReader->next()
    printf("listBatch numElements: %ld\n", listBatch->numElements);
    printf("intBatch numElements: %ld\n", intBatch->numElements);
}

void test_union() {
    std::unique_ptr<orc::OutputStream> outFile = orc::writeLocalFile("file-union.orc");
    orc::MemoryPool *pool = orc::getDefaultPool();
    ORC_UNIQUE_PTR<orc::Type> type(orc::Type::buildTypeFromString("struct<col1:uniontype<int,double,boolean>>"));

    uint64_t stripeSize = 16 * 1024;
    uint64_t compressionBlockSize = 1024;
    // uint64_t rowCount = 3333;
    uint64_t rowCount = 9;

    std::unique_ptr<orc::Writer> writer = myCreateWriter(
                                      stripeSize,
                                      compressionBlockSize,
                                      orc::CompressionKind_NONE,
                                      *type,
                                      pool,
                                      outFile.get(),
                                      orc::FileVersion::v_0_11());

    std::unique_ptr<orc::ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    orc::StructVectorBatch *structBatch = dynamic_cast<orc::StructVectorBatch *>(batch.get());
    orc::UnionVectorBatch *unionBatch = dynamic_cast<orc::UnionVectorBatch *>(structBatch->fields[0]);
    //  * For each value, which element of children has the value.
    unsigned char *tags = unionBatch->tags.data();
    //  * For each value, the index inside of the child ColumnVectorBatch.
    uint64_t *offsets = unionBatch->offsets.data();

    // tags: 0, 1, 2
    orc::LongVectorBatch *intBatch = dynamic_cast<orc::LongVectorBatch *>(unionBatch->children[0]);
    orc::DoubleVectorBatch *doubleBatch = dynamic_cast<orc::DoubleVectorBatch *>(unionBatch->children[1]);
    orc::LongVectorBatch *boolBatch = dynamic_cast<orc::LongVectorBatch *>(unionBatch->children[2]);
    int64_t *intData = intBatch->data.data();
    double *doubleData = doubleBatch->data.data();
    int64_t *boolData = boolBatch->data.data();

    // for keeping track of each union child offset
    uint64_t intOffset = 0, doubleOffset = 0, boolOffset = 0;
    // for specifying the tag of a child
    uint64_t tag = 0;

    for (uint64_t i = 0; i < rowCount; ++i) {
        tags[i] = static_cast<unsigned char>(tag);
        switch(tag) {
            case 0:
                offsets[i] = intOffset;
                intData[intOffset++] = static_cast<int64_t>(i);
            break;
            case 1:
                offsets[i] = doubleOffset;
                doubleData[doubleOffset++] = static_cast<double>(i) + 0.5;
            break;
            case 2:
                offsets[i] = boolOffset;
                boolData[boolOffset++] = (i % 2 == 0) ? 1 : 0;
            break;
        }
        tag = (tag + 1) % 3;
    }

    structBatch->numElements = rowCount;
    unionBatch->numElements = rowCount;
    // the numElements for union children are not set!
    // they should be 1/3 of the rowCount each as seen by the reader

    writer->add(*batch);
    writer->close();
}


void test_timestamp() {
    std::unique_ptr<orc::OutputStream> outFile = orc::writeLocalFile("file-timestamp.orc");
    orc::MemoryPool *pool = orc::getDefaultPool();
    ORC_UNIQUE_PTR<orc::Type> type(orc::Type::buildTypeFromString("struct<col1:timestamp>"));

    uint64_t stripeSize = 16 * 1024;
    uint64_t compressionBlockSize = 1024;
    // uint64_t rowCount = 102400;
    uint64_t rowCount = 10;

    std::unique_ptr<orc::Writer> writer = myCreateWriter(
                                      stripeSize,
                                      compressionBlockSize,
                                      orc::CompressionKind_NONE,
                                      *type,
                                      pool,
                                      outFile.get(),
                                      orc::FileVersion::v_0_11());

    std::unique_ptr<orc::ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    orc::StructVectorBatch *structBatch = dynamic_cast<orc::StructVectorBatch *>(batch.get());
    orc::TimestampVectorBatch *tsBatch = dynamic_cast<orc::TimestampVectorBatch *>(structBatch->fields[0]);

    std::vector<time_t> times(rowCount);
    for (uint64_t i = 0; i < rowCount; ++i) {
        // time_t currTime = -14210715; // 1969-07-20 12:34:45
        time_t currTime = 0; // 1970-01-01 00:00:00 +0000 (UTC)
        times[i] = static_cast<int64_t>(currTime) + static_cast<int64_t >(i * 3660);
        tsBatch->data[i] = times[i];
        tsBatch->nanoseconds[i] = static_cast<int64_t>(i * 1000);
    }
    structBatch->numElements = rowCount;
    tsBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();
}

void test_string() {
    std::unique_ptr<orc::OutputStream> outFile = orc::writeLocalFile("file-string.orc");
    orc::MemoryPool *pool = orc::getDefaultPool();

    // length of strings can vary
    // there is no padding
    // there is no maximum length
    ORC_UNIQUE_PTR<orc::Type> type(orc::Type::buildTypeFromString("struct<col1:string>"));

    uint64_t stripeSize = 16 * 1024; // 16K
    uint64_t compressionBlockSize = 1024; // 1k
    // uint64_t rowCount = 65535;
    uint64_t rowCount = 10;

    std::unique_ptr<orc::Writer> writer = myCreateWriter(
                                      stripeSize,
                                      compressionBlockSize,
                                      orc::CompressionKind_NONE,
                                      *type,
                                      pool,
                                      outFile.get(),
                                      orc::FileVersion::v_0_11());

    std::unique_ptr<orc::ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    orc::StructVectorBatch *structBatch = dynamic_cast<orc::StructVectorBatch *>(batch.get());
    orc::StringVectorBatch *strBatch = dynamic_cast<orc::StringVectorBatch *>(structBatch->fields[0]);

    char scratch[100];
    char dataBuffer[327675];
    uint64_t offset = 0;
    int64_t len;

    for (uint64_t i = 0; i < rowCount; ++i) {
        len = sprintf(scratch, "%ld", i);
        strBatch->data[i] = dataBuffer + offset;
        strBatch->length[i] = static_cast<int64_t>(len);
        memcpy(dataBuffer + offset, scratch, len);
        offset += len;
    }

    structBatch->numElements = rowCount;
    strBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();
}

void test_char() {
    std::unique_ptr<orc::OutputStream> outFile = orc::writeLocalFile("file-char.orc");
    orc::MemoryPool *pool = orc::getDefaultPool();

    // length of strings is fixed
    // if length is < 4 will pad with ' ' (appended)
    // will clip the length of the string to maximum 4 bytes
    ORC_UNIQUE_PTR<orc::Type> type(orc::Type::buildTypeFromString("struct<col1:char(4)>"));

    uint64_t stripeSize = 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 65535;

    std::unique_ptr<orc::Writer> writer = myCreateWriter(
                                      stripeSize,
                                      compressionBlockSize,
                                      orc::CompressionKind_NONE,
                                      *type,
                                      pool,
                                      outFile.get(),
                                      orc::FileVersion::v_0_11());


    std::unique_ptr<orc::ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    orc::StructVectorBatch *structBatch = dynamic_cast<orc::StructVectorBatch *>(batch.get());
    orc::StringVectorBatch *varcharBatch = dynamic_cast<orc::StringVectorBatch *>(structBatch->fields[0]);

    char scratch[100];
    char dataBuffer[327675];
    uint64_t offset = 0;
    int64_t len;

    for (uint64_t i = 0; i < rowCount; ++i) {
        len = sprintf(scratch, "%ld", i);
        varcharBatch->data[i] = dataBuffer + offset;
        varcharBatch->length[i] = static_cast<int64_t>(len);
        memcpy(dataBuffer + offset, scratch, len);
        offset += len;
    }

    structBatch->numElements = rowCount;
    varcharBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();
}

void test_varchar() {
    std::unique_ptr<orc::OutputStream> outFile = orc::writeLocalFile("file-varchar.orc");
    orc::MemoryPool *pool = orc::getDefaultPool();

    // length of strings can vary
    // will clip the length of the string to maximum 4 bytes
    ORC_UNIQUE_PTR<orc::Type> type(orc::Type::buildTypeFromString("struct<col1:varchar(4)>"));

    uint64_t stripeSize = 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 65535;

    std::unique_ptr<orc::Writer> writer = myCreateWriter(
                                      stripeSize,
                                      compressionBlockSize,
                                      orc::CompressionKind_NONE,
                                      *type,
                                      pool,
                                      outFile.get(),
                                      orc::FileVersion::v_0_11());


    std::unique_ptr<orc::ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    orc::StructVectorBatch *structBatch = dynamic_cast<orc::StructVectorBatch *>(batch.get());
    orc::StringVectorBatch *varcharBatch = dynamic_cast<orc::StringVectorBatch *>(structBatch->fields[0]);

    char scratch[100];
    char dataBuffer[327675];
    uint64_t offset = 0;
    int64_t len;

    for (uint64_t i = 0; i < rowCount; ++i) {
        len = sprintf(scratch, "%ld", i);
        varcharBatch->data[i] = dataBuffer + offset;
        varcharBatch->length[i] = static_cast<int64_t>(len);
        memcpy(dataBuffer + offset, scratch, len);
        offset += len;
    }

    structBatch->numElements = rowCount;
    varcharBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();
}

int main(int argc, char const *argv[]) {
    if (argc > 1) {
        switch (argv[1][0]) {
        case '1':
            test_empty();
            break;
        case '2':
            test_one_stripe();
            break;
        case '3':
            // 10 stripes
            // test_multi_stripe(65*1024);
            // test_multi_stripe(1024);
            // test_multi_stripe(512);

            // only 5 stripes
            test_multi_stripe(10);
            break;

        case '4':
            test_array();
            break;

        case '5':
            test_union();
            break;

        case '6':
            test_timestamp();
            break;

        case '7':
            test_string();
            break;

        case '8':
            test_char();
            break;

        case '9':
            test_varchar();
            break;

        default:
            break;
        }
    } else {
        printf("Usage: %s num\n", argv[0]);
    }

    return 0;
}

