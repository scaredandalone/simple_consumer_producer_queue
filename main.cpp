#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <iostream>


const int max_buffer_size = 100;
// shared data
std::queue<int> queue;
// synchronization
std::mutex g_mutex;
std::condition_variable cv_consumer, cv_producer;
// control flags
std::atomic<bool> complete{ false };
// needs to make a lock, wait if the buffer is full, push to buffer if it isnt, and the unlock and notify (sleep at the end).
// protect sections that push to queue and alter control flags (using std::atomic so it should be fine)
void producer(int items_to_produce) {
	for (int i = 0; i < items_to_produce; ++i) {
		std::unique_lock<std::mutex> lock(g_mutex);

		cv_producer.wait(lock, [] {return queue.size() < max_buffer_size; });
		queue.push(i);
		std::cout << "[PRODUCER]: Pushed " << i << " to the Queue! Queue size: " << queue.size() << "\n";

		lock.unlock();
		cv_consumer.notify_one();
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	complete.store(true);
	cv_consumer.notify_all();
}

// make a lock, wait if buffer is empty and producer isnt finished, take the item from the top of the queue (pop) and then unlock. notify and sleep

void consumer() {
	while (true) {
		std::unique_lock<std::mutex> lock(g_mutex);
		cv_consumer.wait(lock, [] {return !queue.empty() || complete; });
		if (queue.empty() && complete) {
			break;
		}
		int item = queue.front();
		queue.pop();
		std::cout << "[CONSUMER]: Popped " << item << " from the Queue! Queue size: " << queue.size() << "\n";
		lock.unlock();
		cv_producer.notify_one();
		std::this_thread::sleep_for(std::chrono::milliseconds(50));

	}
}

int main() {
	const int num_items = 50;
	std::thread producer_thread(producer, num_items);
	std::thread consumer_thread_1(consumer);

	producer_thread.join();
	consumer_thread_1.join();

	std::cout << "Producer and Consumer threads finished." << std::endl;

	return 0;
}
