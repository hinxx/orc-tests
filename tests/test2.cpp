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



void test_model1() {
    std::unique_ptr<orc::OutputStream> outFile = orc::writeLocalFile("file-model1.orc");
    orc::MemoryPool *pool = orc::getDefaultPool();
    ORC_UNIQUE_PTR<orc::Type> type(orc::Type::buildTypeFromString("struct<pvname:string,ts:timestamp,value:uniontype<smallint,int,float,double,string>>"));

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

int main(int argc, char const *argv[]) {
    if (argc > 1) {
        switch (argv[1][0]) {
        case '1':
            test_model1();
            break;

        case '2':
        default:
            break;
        }
    } else {
        printf("Usage: %s num\n", argv[0]);
    }

    return 0;
}

