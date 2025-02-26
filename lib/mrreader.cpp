#include <rdr/mrreader.hpp>

#include <algorithm>
#include <cctype>
#include <iostream>
#include <queue>
#include <stdexcept>
#include <thread>
#include <unordered_map>

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

namespace {

using mapped = std::vector<std::vector<std::pair<std::string, size_t>>>;
using reduced = std::vector<std::pair<std::string, size_t>>;

void MapData(const char* data, size_t start, size_t fin, mapped& words) {
    std::unordered_map<std::string, size_t> dict;

    // Count all the words in range [start, fin] in the dict
    while (start <= fin) {
        while (start <= fin && !std::isalpha(data[start])) {
            ++start;
        }

        size_t cur = start;
        while (cur <= fin && std::isalpha(data[cur])) {
            ++cur;
        }

        if (start < cur) {
            std::string word(data + start, data + cur);
            for (auto& c : word) {
                c = std::tolower(c);
            }
            ++dict[word];
        }

        start = cur;
    }


    // Distribute all words found between $thread_num buckets by hash
    std::hash<std::string> hsh;
    for (auto& [key, value] : dict) {
        words[hsh(key) % words.size()].emplace_back(std::move(key), value);
    }

    // Then sort them for faster merge
    for (auto& bucket : words) {
        std::sort(bucket.begin(), bucket.end());
    }
}

void ReduceData(mapped& mapped_words, reduced& reduced_words) {

    // K-Merge sorted dictionaries in mapped_words into reduced_words
    // summarizing counter on equal words
    auto pq_comp = [](const std::pair<reduced::iterator, reduced::iterator>& a,
                   const std::pair<reduced::iterator, reduced::iterator>& b) {
        return a.first->first > b.first->first;
    };

    std::priority_queue<std::pair<reduced::iterator, reduced::iterator>,
                        std::vector<std::pair<reduced::iterator, reduced::iterator>>,
                        decltype(pq_comp)> pq(pq_comp);

    for (size_t i = 0; i < mapped_words.size(); ++i) {
        if (!mapped_words[i].empty()) {
            pq.emplace(mapped_words[i].begin(), mapped_words[i].end());
        }
    }

    while (!pq.empty()) {
        auto cur = std::move(pq.top());
        pq.pop();

        if (!reduced_words.empty() && reduced_words.back().first == cur.first->first) {
            reduced_words.back().second += cur.first->second;
        } else {
            reduced_words.emplace_back(std::move(*cur.first));
        }

        if (++cur.first != cur.second) {
            pq.emplace(std::move(cur));
        }
    }

    // Sort reduced words by frequency
    auto freq_comp = [](const std::pair<std::string, size_t>& a,
                        const std::pair<std::string, size_t>& b) {
        if (a.second == b.second) {
            return a.first < b.first;
        } else {
            return a.second > b.second;
        }
    };

    std::sort(reduced_words.begin(), reduced_words.end(), freq_comp);
}

} // namespace

MRReader::MRReader(char* from, char* to, size_t thread_count) : thread_count_(thread_count),
                                                                fdin_(-1), fdout_(-1),
                                                                mapped_data_(static_cast<char*>(MAP_FAILED)),
                                                                file_size_(0) {
    Init_(from, to);
}


int MRReader::Run () const {
    std::vector<mapped> mapped_words(thread_count_, mapped(thread_count_, std::vector<std::pair<std::string, size_t>>()));
    std::vector<std::thread> map_threads;

    size_t cur = 0;
    size_t part_size = file_size_ / thread_count_;

    // Map the data by chunks of $part_size size
    for (size_t i = 0; i < thread_count_ && cur <= file_size_; ++i) {
        size_t start = cur;
        size_t fin = std::min(file_size_, start + part_size);
        while (fin < file_size_ && std::isalpha(mapped_data_[fin])) {
            ++fin;
        }
        map_threads.emplace_back(MapData, mapped_data_, start, fin, std::ref(mapped_words[i]));
        cur = fin + 1;
    }

    for (auto& thread : map_threads) {
        thread.join();
    }


    // Transponse matrix so mw[i] contains all buckets with hash reminder equals i
    for (size_t i = 0; i < thread_count_; ++i) {
        for (size_t j = i + 1; j < thread_count_; ++j) {
            std::swap(mapped_words[i][j], mapped_words[j][i]);
        }
    }

    // Reduce data by merging all buckets with equal hash reminder
    std::vector<reduced> reduced_words(thread_count_, reduced());
    std::vector<std::thread> reduce_threads;

    for (size_t i = 0; i < thread_count_; ++i) {
        reduce_threads.emplace_back(ReduceData, std::ref(mapped_words[i]), std::ref(reduced_words[i]));
    }

    for (auto& thread : reduce_threads) {
        thread.join();
    }

    // K-Merge all buckets sorted by frequency and write into fdout

    auto comp = [](const std::pair<reduced::iterator, reduced::iterator>& a,
                   const std::pair<reduced::iterator, reduced::iterator>& b) {
        if (a.first->second == b.first->second) {
            return a.first->first > b.first->first;
        } else {
            return a.first->second < b.first->second;
        }
    };

    std::priority_queue<std::pair<reduced::iterator, reduced::iterator>,
                        std::vector<std::pair<reduced::iterator, reduced::iterator>>,
                        decltype(comp)> pq(comp);

    for (auto& bucket : reduced_words) {
        if (!bucket.empty()) {
            pq.emplace(bucket.begin(), bucket.end());
        }
    }

    while (!pq.empty()) {
        auto cur = std::move(pq.top());
        pq.pop();

        cur.first->first += " " + std::to_string(cur.first->second) + "\n";
        if (write(fdout_, cur.first->first.data(), cur.first->first.length()) < 0) {
            return -1;
        }

        if (++cur.first != cur.second) {
            pq.emplace(std::move(cur));
        }
    }

    return 0;
}

MRReader::~MRReader() {
    if (close(fdin_) < 0) {
        std::cerr << "Error on closing in file\n";
    }
    if (close(fdout_) < 0) {
        std::cerr << "Error on closing out file\n";
    }
    if (munmap(mapped_data_, file_size_) < 0) {
        std::cerr << "Error on munmaping in file\n";
    }
}

void MRReader::Init_(char* from, char* to) {
    fdin_ = open(from, O_RDONLY);
    if (fdin_ < 0) {
        throw std::runtime_error("Unable to open " + std::string(from));
    }

    fdout_ = open(to, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fdout_ < 0) {
        throw std::runtime_error("Unable to create " + std::string(to));
    }

    file_size_ = lseek(fdin_, 0, SEEK_END);
    if (file_size_ < 0 || lseek(fdin_, 0, SEEK_SET) < 0) {
        throw std::runtime_error("lseek failed on " + std::string(from));
    }

    mapped_data_ = static_cast<char*>(mmap(nullptr, file_size_, PROT_READ, MAP_PRIVATE, fdin_, 0));
    if (mapped_data_ == MAP_FAILED) {
        throw std::runtime_error("Unable to mmap file" + std::string(from));
    }
}
