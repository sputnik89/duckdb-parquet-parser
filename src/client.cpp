#include "reader/parquet_reader.hpp"
#include "writer/parquet_writer.hpp"
#include <cassert>
#include <iostream>
#include <map>
#include <vector>
#include <utility>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>

#define SERVER_PORT (8080)

typedef std::pair<char*, uint8_t> string_t;

class StringPageReader {
public:
    StringPageReader(char* bytes, size_t nbytes) :
            bytes_(bytes), nbytes_(nbytes), cursor_(0) {}

    string_t read(size_t offset) {
        if (offset >= nbytes_ || offset == 0) {
            return std::make_pair(nullptr, 0);
        } else {
            uint8_t len = *reinterpret_cast<uint8_t*>(bytes_ + offset - 1);
            char* string = bytes_ + offset;
            return std::make_pair(string, len);
        }
    }
private:
    char* bytes_;
    size_t nbytes_;
    size_t cursor_;
};

int main(int argc, char* argv[]) {
    const std::map<std::string, std::string> columns = {
        {"/home/kaiwen/tpch/part.parquet", "p_type"},
        {"/home/kaiwen/tpch/part.parquet", "p_name"},
        {"/home/kaiwen/tpch/orders.parquet", "o_comment"},
        {"/home/kaiwen/tpch/supplier.parquet", "s_comment"},
    };
    
    for (auto pair : columns) {
        ParquetReader reader;
        reader.open(pair.first);
        auto strings = reader.read_column(pair.second.c_str());
        printf("strings.size()=%lu\n", strings.size());
    }
    
    // if (argc != 3) {
    //     fprintf(stderr, "usage: ./client <filepath> <serverip>\n");
    //     exit(1);
    // }
    
    // int sockfd;
    // struct sockaddr_in server_addr;
    // if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    //     perror("socket");
    //     exit(1);
    // }

    // // configure server address
    // memset(&server_addr, 0, sizeof(server_addr));
    // server_addr.sin_family = AF_INET;
    // server_addr.sin_port = htons(SERVER_PORT);
    // char* serverip = argv[2];
    // if (inet_pton(AF_INET, serverip, &server_addr.sin_addr) <= 0) {
    //     perror("inet_pton");
    //     close(sockfd);
    //     exit(1);
    // }

    // // connect to server
    // if (connect(sockfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
    //     perror("connect");
    //     close(sockfd);
    //     exit(1);
    // }

    // // send pages one by one
    // ParquetReader reader;
    // assert(!reader.open(argv[1]));

    // PageIterator itr = reader.page_iterator();
    // while (itr.has_next()) {
    //     RawPage page = itr.next();
    //     std::vector<uint8_t> page_data = page.data;
    //     char* msg = reinterpret_cast<char*>(page_data.data());
    //     size_t nbytes_total = page_data.size();
    //     size_t nbytes_sent = 0;
    //     ssize_t sent;
    //     // send all of the current page to server
    //     while (nbytes_sent < nbytes_total) {
    //         if ((sent = send(sockfd, msg+nbytes_sent, nbytes_total-nbytes_sent, 0)) < 0) {
    //             exit(1);
    //         }
    //         nbytes_sent += sent;
    //     }
    //     // wait for DPK regex to finish preprocessing and read the outputs

    //     // TODO: read all of the current response from server
    //     std::vector<std::string> matched_strings;
    //     StringPageReader page_reader(reinterpret_cast<char*>(page_data.data()), nbytes_total);
    //     size_t offsets[] = {};  // TODO:
    //     size_t n_matches = 0;  // TODO:
    //     for (size_t i = 0; i < n_matches; i++) {
    //         string_t match = page_reader.read(offsets[i]);
    //         if (match.first) {
    //             matched_strings.emplace_back(match.first, match.second);
    //         }
    //     }

    //     // if (!matched_strings.empty()) {
    //     //     std::vector<Value> col_values;
    //     //     for (const auto& s : matched_strings) {
    //     //         col_values.push_back(Value::from_string(s));
    //     //     }
    //     //     writer.write_row_group({col_values});
    //     // }
    // }

    // // writer.close();
}
