import string
import os
import psutil
from collections import Counter
import time
import threading
from multiprocessing import Pool, cpu_count
from concurrent.futures import ProcessPoolExecutor
import mmap
#get stop words
def get_stop_words(path):
    stop_words = list()
    with open(path, "r") as file:
        lines = file.readlines()
    for line in lines:
        #delete line break
        stop_words.append(line.strip())
    
    return stop_words

#get text data
def get_text_data(path):
    f = open(path)
    lines = f.read()
    f.close()
    return lines

#remove punctuation
def remove_punctuation(text):
    text = text.translate(str.maketrans('','',string.punctuation))
    return text

#split
def split_text(text):
    return text.split()

#filter
def filter_stop_words(text, stop_words):
    return [word for word in text if word not in stop_words]

def read_chunks(file, buffer_size):
    chunks = []
    start_pos = 0

    while True:
        #read from the start point
        file.seek(start_pos)
        data = file.read(buffer_size)
        if not data:
            break

        while True:
            last_char = file.read(1)
            if last_char in {os.linesep, ''}:
                break
            data += last_char
        #update start point and endpoint
        end_pos = start_pos + len(data)
        chunks.append((start_pos, end_pos))
        start_pos = end_pos

    return chunks


# def count_words(lines, stopwords = None):

#     chunk = ' '.join(lines)
#     processed_text = remove_punctuation(chunk)
#     word_list = split_text(processed_text)
#     if stopwords:
#         word_list = filter_stop_words(word_list, stopwords)
    
#     return Counter(word_list)

def count_words(file_path, start_pos, end_pos, stopwords):
    with open(file_path, "r", encoding='utf-8') as f:
        with mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ) as mmaped_file:
            mmaped_file.seek(start_pos)
            chunk = mmaped_file.read(end_pos - start_pos).decode('utf-8')

    chunk = chunk.lower()
    processed_text = remove_punctuation(chunk)
    word_list = split_text(processed_text)
    word_list = filter_stop_words(word_list, stopwords)
    
    return Counter(word_list)


def find_top_k_words_large_file(file_path, k, buffer_size, stopwords):
    with open(file_path, "r", encoding='utf-8') as f:
        chunks = read_chunks(f, buffer_size)

    with Pool(cpu_count()) as pool:
        counters = pool.starmap(count_words, [(file_path, start, end, stopwords) for start, end in chunks])

    # with ProcessPoolExecutor(max_workers=cpu_count()) as executor:
    #     counters = list(executor.map(count_words, [(file_path, start, end, stopwords) for start, end in chunks]))

    total_word_count = Counter()
    for counter in counters:
        total_word_count.update(counter)

    #return total_word_count.most_common(k)
    #This is the code of applying minheap, it doesn't work as well as the counter so we decide we won't apply it anymore. This is just for presenting
    # minheap = []
    # for word, count in total_word_count.items():
    #     if len(minheap) < k:
    #         heapq.heappush(minheap, (count, word))
    #     else:
    #         if count > minheap[0][0]:
    #             heapq.heappop(minheap)
    #             heapq.heappush(minheap, (count, word))

    # res = [heapq.heappop(minheap)[1] for _ in range(len(minheap))][::-1]
    # return res

    return total_word_count.most_common(k)

#sample resource usage on the machine
def sampling(stop_event):
    while not stop_event.is_set():
        cpu_percent = psutil.cpu_percent()
        mem = psutil.virtual_memory()
        cpu_percent_list.append(cpu_percent)
        mem_list.append(mem.used)
        print("Sampling: CPU: {:.2f}%, MEM: {:.2f}MB".format(cpu_percent, mem.used / 1024 / 1024))  # Add this line

        time.sleep(3)

def exit_handler():
    if len(cpu_percent_list) > 0:
        cpu_average_percent = sum(cpu_percent_list) / len(cpu_percent_list)
    else:
        cpu_average_percent = 0
    if len(mem_list) > 0:
        mem_max_usage = max(mem_list) / 1024 / 1024
    else:
        mem_max_usage = 0
    print("Average cpu percentage: {:.2f}%".format(cpu_average_percent))
    print("Maximum memory usage: {:.2f}MB".format(mem_max_usage))

if __name__ == '__main__':
    #start time
    start_time = time.perf_counter()
    current_dir = os.getcwd()
    stop_words_path = os.path.join(current_dir, "data", "stop-words", "NLTK's list of english stopwords.txt")
    text_data_path = os.path.join(current_dir, "data", "dataset_updated","data_300MB.txt")

    cpu_percent_list = []
    mem_list = []
    #start sampling thread
    stop_event = threading.Event()
    sampling_thread = threading.Thread(target=sampling, args=(stop_event,))
    sampling_thread.start()

    #set parameters
    k = 10
    buffer_size = 25 * 1024 * 1024
    #get stop words
    stop_words = get_stop_words(stop_words_path)
    print("stop words lenth: " + str(len(stop_words)))
    #find top k
    result = find_top_k_words_large_file(text_data_path, k, buffer_size, stop_words)
    print(result)
    #stop resource monitor thread
    stop_event.set()
    sampling_thread.join()

    exit_handler()

    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print("Time used: " + str(elapsed_time))




