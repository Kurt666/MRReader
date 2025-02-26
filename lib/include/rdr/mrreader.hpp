#include <string>
#include <utility>
#include <vector>

namespace {
    using mapped = std::vector<std::vector<std::pair<std::string, size_t>>>;
    using reduced = std::vector<std::pair<std::string, size_t>>;

    void MapData(const char* data, size_t start, size_t fin, mapped& words);

    void ReduceData(mapped& mapped_words, reduced& reduced_words);
} // namespace

class MRReader {
public:
    explicit MRReader(char* from, char* to, size_t thread_num);
    int Run() const;
    ~MRReader();

private:
    void Init_(char* from, char* to);

    int fdin_;
    int fdout_;
    char* mapped_data_;
    size_t file_size_;
    size_t thread_count_;
};


