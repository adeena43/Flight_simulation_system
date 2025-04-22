#include <iostream>
#include <string>
#include <queue>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <fstream>
#include <sstream>
#include <chrono>
#include <ctime>
#include <future>
#include <atomic>
#include <unistd.h> // For sleep() function
using namespace std;

class Flight
{
public:
    string flightID;
    int priority;
    int departureTime;
    int landingTime;
    string route;

    Flight(const string &flightId, int priority, int depTime, int landTime, const string &route)
        : flightID(flightId), priority(priority), departureTime(depTime), landingTime(landTime), route(route) {}
};

struct FlightComparator
{
    bool operator()(const shared_ptr<Flight> &a, const shared_ptr<Flight> &b)
    {
        return a->priority > b->priority; // Higher priority flights get served first
    }
};

class Runway
{
protected:
    priority_queue<shared_ptr<Flight>, vector<shared_ptr<Flight>>, FlightComparator> queue;
    mutex mtx;
    condition_variable cv;
    atomic<bool> shutdownRequested = false;

public:
    // Enqueue a flight in the priority queue
    virtual void enqueue(const shared_ptr<Flight> &flight)
    {
        lock_guard<mutex> lock(mtx);
        queue.push(flight);
        cv.notify_one(); // Notify waiting threads
    }

    shared_ptr<Flight> dequeue()
    {
        unique_lock<mutex> lock(mtx);
        cv.wait(lock, [this]
                { return !queue.empty() || shutdownRequested; });

        if (shutdownRequested)
            return nullptr;
        if (queue.empty())
            return nullptr;

        auto flight = queue.top();
        queue.pop();
        return flight;
    }

    void requestShutdown()
    {
        shutdownRequested = true;
        cv.notify_all(); // Notify all waiting threads to wake up and check the shutdown flag
    }
};

class Runway1 : public Runway
{ // Takeoff queue/operations
public:
    void process_takeoffs()
    {
        while (true)
        {
            shared_ptr<Flight> flight;

            {
                unique_lock<mutex> lock(mtx);
                cv.wait(lock, [this]
                        { return !queue.empty() || shutdownRequested; });

                if (shutdownRequested && queue.empty())
                {
                    cout << "Shutdown requested. Exiting process_takeoffs()." << endl;
                    return;
                }

                if (queue.empty())
                {
                    requestShutdown();
                    continue;
                }

                flight = queue.top();
                queue.pop();
            }

            // Process the takeoff of the flight
            if (flight->priority == 1)
            { // Emergency handling
                lock_guard<mutex> lock(mtx);
                cout << "\nEmergency landing: delaying takeoffs for flight " << flight->flightID << endl;
                this_thread::sleep_for(chrono::seconds(flight->landingTime)); // Emergency delay
            }
            else
            {
                lock_guard<mutex> lock(mtx);
                cout << "\nTaking off flight " << flight->flightID << " with priority " << flight->priority << endl;
                this_thread::sleep_for(chrono::seconds(flight->departureTime)); // Simulate takeoff time
            }
        }
    }
};

class Runway2 : public Runway
{ // Landing queue/operations
public:
    void process_landings()
    {
        while (true)
        {
            shared_ptr<Flight> flight;

            {
                unique_lock<mutex> lock(mtx);
                cv.wait(lock, [this]
                        { return !queue.empty() || shutdownRequested; });

                if (shutdownRequested && queue.empty())
                {
                    cout << "\nShutdown requested. Exiting process_landings()." << endl;
                    return;
                }

                if (queue.empty())
                {
                    requestShutdown();
                    continue;
                }

                flight = queue.top();
                queue.pop();
            }

            lock_guard<mutex> lock(mtx);
            cout << "\nLanding flight " << flight->flightID << " with priority " << flight->priority << endl;
            this_thread::sleep_for(chrono::milliseconds(flight->landingTime)); // Simulate landing time
        }
    }
};

// Function to parse a CSV line into a vector of strings
vector<string> parseCSVLine(const string &line)
{
    vector<string> result;
    istringstream iss(line);
    string item;
    while (getline(iss, item, ','))
    {
        result.push_back(item);
    }
    return result;
}

// Data Parallelism: Distributes flights into appropriate queues
void handleDataParallelism(const string &filename, Runway1 &takeoffQueue, Runway2 &landingQueue)
{
    ifstream file(filename);
    if (!file.is_open())
    {
        cerr << "Error opening file: " << filename << endl;
        return;
    }

    string line;
    while (getline(file, line))
    {
        vector<string> parts = parseCSVLine(line);
        if (parts.size() == 5)
        {
            string flightID = parts[0];
            int priority = stoi(parts[1]);
            int departureTime = stoi(parts[2]);
            int landingTime = stoi(parts[3]);
            string route = parts[4];

            auto flight = make_shared<Flight>(flightID, priority, departureTime, landingTime, route);

            if (departureTime > landingTime)
            {
                takeoffQueue.enqueue(flight);
            }
            else
            {
                landingQueue.enqueue(flight);
            }
        }
        else
        {
            cerr << "Invalid data format: " << line << endl;
        }
    }

    file.close();
    takeoffQueue.requestShutdown();
    landingQueue.requestShutdown();
}

// Task Parallelism: Processes tasks concurrently
void handleTaskParallelism(const string &filename, Runway1 &takeoffQueue, Runway2 &landingQueue)
{
    ifstream file(filename);
    if (!file.is_open())
    {
        cerr << "Error opening file: " << filename << endl;
        return;
    }

    string line;
    while (getline(file, line))
    {
        vector<string> parts = parseCSVLine(line);
        if (parts.size() == 5)
        {
            string flightID = parts[0];
            int priority = stoi(parts[1]);
            int departureTime = stoi(parts[2]);
            int landingTime = stoi(parts[3]);
            string route = parts[4];

            auto flight = make_shared<Flight>(flightID, priority, departureTime, landingTime, route);

            if (departureTime > landingTime)
            {
                takeoffQueue.enqueue(flight); // Takeoff queue
            }
            else
            {
                landingQueue.enqueue(flight); // Landing queue
            }
        }
        else
        {
            cerr << "Invalid data format: " << line << endl;
        }
    }

    file.close();
    takeoffQueue.requestShutdown();
    landingQueue.requestShutdown();
}

int main()
{
    Runway1 takeoffQueue;
    Runway2 landingQueue;

    // Sample CSV file (provide the path accordingly)
    string filename = "flights.csv";

    // Launch parallel processing threads
    thread dataThread(handleDataParallelism, filename, ref(takeoffQueue), ref(landingQueue));
    thread taskThread(handleTaskParallelism, filename, ref(takeoffQueue), ref(landingQueue));

    // Run runway processing threads
    thread takeoffThread(&Runway1::process_takeoffs, &takeoffQueue);
    thread landingThread(&Runway2::process_landings, &landingQueue);

    // Wait for all threads to complete
    dataThread.join();
    taskThread.join();
    takeoffThread.join();
    landingThread.join();

    cout << "All processes completed successfully." << endl;
    return 0;
}
