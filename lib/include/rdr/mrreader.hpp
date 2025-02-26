#include <string>
#include <utility>
#include <vector>

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


