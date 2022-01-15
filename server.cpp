#include "kv.pb.h"
#include "log.h"
#include "protocol.h"
#include "rpc.h"

#include <array>
#include <cstdio>
#include <cstring>
#include <sstream>
#include <string>
#include <unordered_map>

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <unistd.h>

#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>

static_assert(EAGAIN == EWOULDBLOCK);

using namespace NLogging;
using namespace NProtocol;
using namespace NRpc;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr int max_events = 32;

////////////////////////////////////////////////////////////////////////////////

auto create_and_bind(std::string const& port)
{
    struct addrinfo hints;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC; /* Return IPv4 and IPv6 choices */
    hints.ai_socktype = SOCK_STREAM; /* TCP */
    hints.ai_flags = AI_PASSIVE; /* All interfaces */

    struct addrinfo* result;
    int sockt = getaddrinfo(nullptr, port.c_str(), &hints, &result);
    if (sockt != 0) {
        LOG_ERROR("getaddrinfo failed");
        return -1;
    }

    struct addrinfo* rp = nullptr;
    int socketfd = 0;
    for (rp = result; rp != nullptr; rp = rp->ai_next) {
        socketfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (socketfd == -1) {
            continue;
        }

        sockt = bind(socketfd, rp->ai_addr, rp->ai_addrlen);
        if (sockt == 0) {
            break;
        }

        close(socketfd);
    }

    if (rp == nullptr) {
        LOG_ERROR("bind failed");
        return -1;
    }

    freeaddrinfo(result);

    return socketfd;
}

////////////////////////////////////////////////////////////////////////////////

auto make_socket_nonblocking(int socketfd)
{
    int flags = fcntl(socketfd, F_GETFL, 0);
    if (flags == -1) {
        LOG_ERROR("fcntl failed (F_GETFL)");
        return false;
    }

    flags |= O_NONBLOCK;
    int s = fcntl(socketfd, F_SETFL, flags);
    if (s == -1) {
        LOG_ERROR("fcntl failed (F_SETFL)");
        return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

SocketStatePtr accept_connection(
    int socketfd,
    struct epoll_event& event,
    int epollfd)
{
    struct sockaddr in_addr;
    socklen_t in_len = sizeof(in_addr);
    int infd = accept(socketfd, &in_addr, &in_len);
    if (infd == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return nullptr;
        } else {
            LOG_ERROR("accept failed");
            return nullptr;
        }
    }

    std::string hbuf(NI_MAXHOST, '\0');
    std::string sbuf(NI_MAXSERV, '\0');
    auto ret = getnameinfo(
        &in_addr, in_len,
        const_cast<char*>(hbuf.data()), hbuf.size(),
        const_cast<char*>(sbuf.data()), sbuf.size(),
        NI_NUMERICHOST | NI_NUMERICSERV);

    if (ret == 0) {
        LOG_INFO_S("accepted connection on fd " << infd
            << "(host=" << hbuf << ", port=" << sbuf << ")");
    }

    if (!make_socket_nonblocking(infd)) {
        LOG_ERROR("make_socket_nonblocking failed");
        return nullptr;
    }

    event.data.fd = infd;
    event.events = EPOLLIN | EPOLLOUT | EPOLLET;
    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, infd, &event) == -1) {
        LOG_ERROR("epoll_ctl failed");
        return nullptr;
    }

    auto state = std::make_shared<SocketState>();
    state->fd = infd;
    return state;
}

}   // namespace


////////////////////////////////////////////////////////////////////////
class HashTable {
private:
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>> table[2];
    std::thread write_thread;
    std::string log_file[2] = {"log1.log", "log2.log"};
    std::string table_file[2] = {"table1.tab", "table2.tab"};
    std::ofstream log_out;
    std::mutex mutex;
    uint64_t active_ind = 0;
    uint64_t current_version = 1;
    uint64_t flush_size = -1;
    bool running;	
    
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>& active() {
         return table [active_ind % 2];
    }
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>& inactive() {
         return table [(active_ind + 1) % 2];
    }

    void write_thread_func() {
        while (running) {
            std::ofstream table_out(table_file[(active_ind + 1) % 2], std::ofstream::out | std::ofstream::trunc);
            uint64_t counter = 0;
            for (auto& it: inactive()) {
                table_out << it.first << " " << it.second.first << " " << it.second.second << "\n";
                if (flush_size > 0 && flush_size >= counter) {
                    table_out.flush();
                    counter = 0;
                }
            }
            table_out.flush();
            table_out.close();
            std::this_thread::sleep_for(std::chrono::seconds(1));

            std::lock_guard<std::mutex> g(mutex);
            active_ind++;
            log_out.flush();
            log_out.close();
            log_out.open(log_file[active_ind % 2], std::ofstream::out | std::ofstream::trunc);
        }
    }



    void restore() {
	for(int i = 0; i < 2; i++){
            std::ifstream table_in(table_file[i]);
    	    std::ifstream log_in(log_file[i]);

   	    std::string key;
    	    std::uint64_t value;
	    std::uint64_t version;
    	    while (table_in >> key >> value >> version) {
                table[i][key] = {value, version};    
                if (version + 1 > current_version){
                    current_version = version + 1;
                }
    	   }

    	    while (log_in >> key >> value >> version) {
                table[i][key] = {value, version};    
                if (version + 1 > current_version){
                    current_version = version + 1;
                }
    	   }

    	    table_in.close();
    	    log_in.close();
    }

}

public:
    HashTable() {
        std::lock_guard<std::mutex> g(mutex);
	running = true;
     	restore();
	log_out.open(log_file[active_ind % 2], std::ofstream::trunc);
        write_thread = std::thread([this] { write_thread_func(); });

    }
    ~HashTable(){
        running = false;
        write_thread.join();
        log_out.flush();
        log_out.close();
    }

    std::pair<bool, uint64_t> find(const std::string &key) {
        std::lock_guard<std::mutex> g(mutex);
	std::pair<uint64_t, uint64_t> value;
	for(int i = 0; i < 2; i++){
            auto it = table[i].find(key);
		if (it != table[i].end() && it->second.second > value.second) {
                value = it->second;
            }
        }
        return std::make_pair(value.second != 0, value.first);
    }

    void insert(const std::string &key, const uint64_t &value) {
        std::lock_guard<std::mutex> g(mutex);
        log_out << key << " " << value << current_version <<"\n";
        table[active_ind % 2][key] = {value, current_version};
	current_version++;
    }


};



////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    if (argc < 2) {
        return 1;
    }

    /*
     * socket creation and epoll boilerplate
     * TODO extract into struct Bootstrap
     */

    auto socketfd = ::create_and_bind(argv[1]);
    if (socketfd == -1) {
        return 1;
    }

    if (!::make_socket_nonblocking(socketfd)) {
        return 1;
    }

    if (listen(socketfd, SOMAXCONN) == -1) {
        LOG_ERROR("listen failed");
        return 1;
    }

    int epollfd = epoll_create1(0);
    if (epollfd == -1) {
        LOG_ERROR("epoll_create1 failed");
        return 1;
    }

    struct epoll_event event;
    event.data.fd = socketfd;
    event.events = EPOLLIN | EPOLLET;
    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, socketfd, &event) == -1) {
        LOG_ERROR("epoll_ctl failed");
        return 1;
    }

    /*
     * handler function
     */

    // TODO on-disk storage
    HashTable storage;
    
    
    auto handle_get = [&] (const std::string& request) {
        NProto::TGetRequest get_request;
        if (!get_request.ParseFromArray(request.data(), request.size())) {
            // TODO proper handling

            abort();
        }

        LOG_DEBUG_S("get_request: " << get_request.ShortDebugString());

        NProto::TGetResponse get_response;
        get_response.set_request_id(get_request.request_id());
        auto it = storage.find(get_request.key());
        if (it.first) {
            get_response.set_offset(it->second);
        }

        std::stringstream response;
        serialize_header(GET_RESPONSE, get_response.ByteSizeLong(), response);
        get_response.SerializeToOstream(&response);

        return response.str();
    };

    auto handle_put = [&] (const std::string& request) {
        NProto::TPutRequest put_request;
        if (!put_request.ParseFromArray(request.data(), request.size())) {
            // TODO proper handling

            abort();
        }

        LOG_DEBUG_S("put_request: " << put_request.ShortDebugString());

        storage.insert(put_request.key(), put_request.offset());


        NProto::TPutResponse put_response;
        put_response.set_request_id(put_request.request_id());

        std::stringstream response;
        serialize_header(PUT_RESPONSE, put_response.ByteSizeLong(), response);
        put_response.SerializeToOstream(&response);

        return response.str();
    };

    Handler handler = [&] (char request_type, const std::string& request) {
        switch (request_type) {
            case PUT_REQUEST: return handle_put(request);
            case GET_REQUEST: return handle_get(request);
        }

        // TODO proper handling

        abort();
        return std::string();
    };

    /*
     * rpc state and event loop
     * TODO extract into struct Rpc
     */

    std::array<struct epoll_event, ::max_events> events;
    std::unordered_map<int, SocketStatePtr> states;

    auto finalize = [&] (int fd) {
        LOG_INFO_S("close " << fd);

        close(fd);
        states.erase(fd);
    };

    while (true) {
        const auto n = epoll_wait(epollfd, events.data(), ::max_events, -1);

        {
            LOG_INFO_S("got " << n << " events");
        }

        for (int i = 0; i < n; ++i) {
            const auto fd = events[i].data.fd;

            if (events[i].events & EPOLLERR
                    || events[i].events & EPOLLHUP
                    || !(events[i].events & (EPOLLIN | EPOLLOUT)))
            {
                LOG_ERROR_S("epoll event error on fd " << fd);

                finalize(fd);

                continue;
            }

            if (socketfd == fd) {
                while (true) {
                    auto state = ::accept_connection(socketfd, event, epollfd);
                    if (!state) {
                        break;
                    }

                    states[state->fd] = state;
                }

                continue;
            }

            if (events[i].events & EPOLLIN) {
                auto state = states.at(fd);
                if (!process_input(*state, handler)) {
                    finalize(fd);
                }
            }

            if (events[i].events & EPOLLOUT) {
                auto state = states.at(fd);
                if (!process_output(*state)) {
                    finalize(fd);
                }
            }
        }
    }

    LOG_INFO("exiting");

    close(socketfd);

    return 0;
}
