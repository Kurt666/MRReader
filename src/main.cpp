#include <rdr/mrreader.hpp>

#include <iostream>
#include <memory>
#include <thread>


int main(int argc, char *argv[]) {
    void *src, *dst;

    if (argc != 3) {
        std::cerr << "Usage: " <<  std::string(argv[0]) <<  " <fromfile> <tofile>" << std::endl;
        return 1;
    }

    std::unique_ptr<MRReader> reader;

    try {
        reader = std::make_unique<MRReader>(argv[1], argv[2], std::thread::hardware_concurrency());
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }

    if (reader->Run() < 0) {
        std::cerr << "Error writing in output file\n";
        return 1;
    }

    return 0;
}
