import threading
import pickle 
import glob
import time, os

class Website(object):
    """docstring for Website"""
    def __init__(self, url, meta_data = None, crawled = False):
        super(Website, self).__init__()
        self.url = url
        self.meta_data = meta_data
        self.crawled = crawled
        
    def __eq__(self, other):
        if isinstance(other, Website):
            return self.url == other.url
        return NotImplemented

    def __hash__(self):
        return hash(self.url)

    def __str__(self):
        return 'The url is "{}", which has {}been crawled'.format(self.url, '' if self.crawled else "NOT ")
  
    def __repr__(self):
        return 'The url is "{}", which has {}been crawled'.format(self.url, '' if self.crawled else "NOT ")

    def set_crawled(self):
        self.crawled = True

class Database(object):
    """docstring for Database"""
    def __init__(self, dir_path, chunk_size):
        super(Database, self).__init__()
        
        assert not os.path.exists(dir_path), \
            'The database path "{}" exist, try resume?'.format(dir_path)

        self.dir_path = dir_path
        os.mkdir(dir_path)
        os.mkdir(self.get_cache_path_())

        self.chunk_size = chunk_size
        self.total_record_num = 0

        self.current_file_index = 0
        self.cache_record_num = 0


    def insert(self, records):

        with open(self.current_cache_file_path_(), 'w') as f:
            json.dump(records, f)

        self.cache_record_num += len(records)
        self.merge_cache_if_need_()


    def merge_cache_if_need_(self):

        if self.cache_record_num < self.chunk_size: return

        self.merge_cache_()
        self.empty_cache_dir_()

        print('we clear the cache and save data to {}'.format(self.current_data_file_path_()))

        self.total_record_num += self.cache_record_num
        self.cache_record_num = 0
        self.current_file_index += 1


    def merge_cache_(self):
        data = []
        for cache_file in glob.glob('{}/_cache/*.json'.format(self.dir_path)):
            with open(cache_file, 'r') as f:
                data += json.load(f)

        with open(self.current_data_file_path_(), 'w') as f:
            json.dump(data, f)



    def get_cache_path_(self):
        return '{}/_cache'.format(self.dir_path)

    def current_data_file_path_(self):
        return '{}/{}.json'.format(self.dir_path, self.current_file_index)

    def current_cache_file_path_(self):
        return '{}/_cache/{}.json'.format(self.dir_path, self.cache_record_num)

    def empty_cache_dir_(self):
        os.system("rm {}/*".format(self.get_cache_path_()))


    def __repr__(self):
        return 'The dir path is {}, and the current data file index is {}, \nthere are {} cached records and the {} records in database.'.format(
                    self.dir_path, self.current_file_index,
                    self.cache_record_num, self.total_record_num)

class WorkSpace(object):
    """docstring for WorkSpace"""
    def __init__(self, path):
        super(WorkSpace, self).__init__()
        
        self.path = path

        if os.path.exists(path):
            with open(path, 'rb') as f:
                self.meta_urls, self.working_urls, self.db = pickle.load(f)
            self.print_info()
        else:
            self.working_urls = set()
            self.meta_urls = set()
            self.db = Database('db', 16384)
            self.save()
            
        self._regular_backup()

    def save(self):     
        with open(self.path, 'wb') as f:
            pickle.dump((self.meta_urls, self.working_urls, self.db), f)

    def uncrawled_working_urls_all(self):
        return list(filter(lambda x: not x.crawled, self.working_urls))

    def append_working_urls(self, urls):
        assert all(map(lambda x: isinstance(x, Website), urls)), \
            "All urls should be instance of Website"
        self.working_urls |= (set(urls) - self.working_urls)

    def remove_working_urls(self, urls):
        assert all(map(lambda x: isinstance(x, Website), urls)), \
            "All urls should be instance of Website"
        self.working_urls - set(urls)

    def remove_working_url(self, url):
        isinstance(url, Website), "Url should be instance of Website"
        self.working_urls.remove(url)



    def add_crawled_meta_url(self, url):
        website = Website(url, crawled = True)
        self.meta_urls.add(website)

    def has_crawled_this_meta_url(self, url):
        return Website(url) in self.meta_urls


    def _regular_backup(self, interval = 7200):     

        threading.Timer(interval, self._regular_backup)

        with open(self.path + '.bk', 'wb') as f:
            pickle.dump((self.meta_urls, self.working_urls, self.db), f)

    def print_info(self):
        print("Util now, we have crawled {} meta urls and remains {} working urls".format(
            len(self.meta_urls), len(self.working_urls)))
        print("Database: ", str(self.db))













