import os, sys, logging, time, csv
import urllib.request, shutil
import basc_py4chan
import redis
from flask import Flask

FOURC_LOOP_DELAY = 'FOURC_LOOP_DELAY'
FOURC_TASK_DELAY = 'FOURC_TASK_DELAY'
FOURC_BOARD = 'FOURC_BOARD'
FOURC_FUNCTION = 'FOURC_FUNCTION'
FOURC_NFSPATH = 'FOURC_NFSPATH'

FUNCTION_BOARD_THREAD_LIVENESS = 'board_thread_liveness_update'
FUNCTION_THREAD_UPDATE = 'thread_update'
FUNCTION_POST_IMAGE_FETCH = 'post_image_fetch'

THREAD_CLOSED = 'closed'
THREAD_STICKY = 'sticky'
THREAD_ARCHIVED = 'archived'
THREAD_BUMPLIMIT = 'bumplimit'
THREAD_IMAGELIMIT = 'imagelimit'
THREAD_TOPIC = 'topic'
THREAD_URL = 'url'
THREAD_SEMANTIC_URL = 'semantic_url'
THREAD_SEMANTIC_SLUG = 'semantic_slug'
THREAD_NUM_POSTS = 'num_posts'
THREAD_POST_LIST_NAME = 'post_list_name'
THREAD_IMAGE_LIST_FILENAME = 'image_list_filename'

# we set up the logger to output everything to stdout, suitable for PCF deployment
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# this NFS mount holds all application data (posts, images, etc)
nfs_mount = '/Users/colrich/nfs/4c-dev/'

# this is the function that this process will execute. possible values are
# enumerated in the table below
process_function = os.environ[FOURC_FUNCTION]
loop_delay = int(os.environ[FOURC_LOOP_DELAY])
task_delay = int(os.environ[FOURC_TASK_DELAY])
board_name = os.environ[FOURC_BOARD]
nfs_mount_path = os.environ[FOURC_NFSPATH]


def thread_hash_key(board_name, tid):
    return '4c:' + board_name + ':threads:' + str(tid)

def thread_post_set_name(board_name, tid):
    return '4c:' + board_name + ':threads:' + str(tid) + ':all_post_ids'

def nfs_path():
    return nfs_mount_path

def board_path(board_name):
    return nfs_path() + board_name + '/'

def thread_path(board_name, tid):
    return board_path(board_name) + str(tid) + '/'

def post_file_path(board_name, tid, ext):
    return thread_path(board_name, tid) + 'posts.' + ext

def post_images_file_path(board_name, tid, ext):
    return thread_path(board_name, tid) + 'images.' + ext

def post_image_path(board_name, tid, pid, img_name):
    return thread_path(board_name, tid) + str(pid) + '-' + img_name


logger.debug('board: /%s/ / function: %s / loop delay: %is / task delay: %is', \
    board_name, process_function, loop_delay, task_delay)


BOARD_INDEX_KEY = '4c:' + board_name + ':thread_check_set'
IMAGE_FETCH_INDEX_KEY = '4c:' + board_name + ':image_fetch'

# this redis instance holds all application state
rds = redis.StrictRedis(host="10.88.191.17", port=32226, db=0)

# this board object lets us interact with the board via 4chan api
board = basc_py4chan.Board(board_name)


if process_function == FUNCTION_BOARD_THREAD_LIVENESS:
    while 1:
        # this method enumerates all of the currently active thread ids and adds them to a
        # redis set; they'll be picked up from there by the next function in the pipeline
        logger.debug('FOURC_FUNCTION set to ' + FUNCTION_BOARD_THREAD_LIVENESS + '; beginning execution...')

        # add all current thread ids to the set; this is fine (no duplicates will result) 
        # because this is how Redis sets work
        count = 0
        for tid in board.get_all_thread_ids():
            rds.sadd(BOARD_INDEX_KEY, str(tid))
            count += 1
        logger.debug(FUNCTION_BOARD_THREAD_LIVENESS + ' added %i thread ids to ' + BOARD_INDEX_KEY, count)

        logger.debug(FUNCTION_BOARD_THREAD_LIVENESS + ' run completed')
        time.sleep(loop_delay)
elif process_function == FUNCTION_THREAD_UPDATE:
    while 1:
        # this method pulls threads off the board liveness index and checks the thread. we update it
        # each time it's touched, until it's closed. at that time all the thread info is dumped
        logger.debug('FOURC_FUNCTION set to ' + FUNCTION_THREAD_UPDATE + '; beginning execution...')

        # take a thread id at random from the set
        thrid = rds.spop(BOARD_INDEX_KEY)
        if thrid is None:
            logger.debug(FUNCTION_THREAD_UPDATE + ' board index empty, none result from popping index')
            time.sleep(task_delay)
            continue

        tid = int(thrid)
        logger.debug(FUNCTION_THREAD_UPDATE + ' running on tid: %i', tid)
        thread = board.get_thread(tid)

        # check that valid thread object returned from api; if not, just loop around and pull another
        if thread is None:
            logger.debug('thread: ' + str(tid) + ' not found on board, pruning')
            continue

        # set all thread metadata for this thread, overwriting if exists
        logger.debug(FUNCTION_THREAD_UPDATE + ' setting metadata on tid: %i', tid)
        rds.hset(thread_hash_key(board_name, tid), THREAD_STICKY, str(thread.sticky))
        rds.hset(thread_hash_key(board_name, tid), THREAD_CLOSED, str(thread.closed))
        rds.hset(thread_hash_key(board_name, tid), THREAD_ARCHIVED, str(thread.archived))
        rds.hset(thread_hash_key(board_name, tid), THREAD_BUMPLIMIT, str(thread.bumplimit))
        rds.hset(thread_hash_key(board_name, tid), THREAD_IMAGELIMIT, str(thread.imagelimit))
        rds.hset(thread_hash_key(board_name, tid), THREAD_URL, thread.url)
        rds.hset(thread_hash_key(board_name, tid), THREAD_SEMANTIC_URL, thread.semantic_url)
        rds.hset(thread_hash_key(board_name, tid), THREAD_SEMANTIC_SLUG, thread.semantic_slug)
        rds.hset(thread_hash_key(board_name, tid), THREAD_NUM_POSTS, str(len(thread.all_posts)))
        rds.hset(thread_hash_key(board_name, tid), THREAD_TOPIC, str(thread.topic.post_id))
        rds.hset(thread_hash_key(board_name, tid), THREAD_IMAGE_LIST_FILENAME, post_images_file_path(board_name, tid, 'csv'))
        
        # write out all posts into a csv
        logger.debug(FUNCTION_THREAD_UPDATE + ' writing posts to path: ' + post_file_path(board_name, tid, 'csv') + ' on tid: %i', tid)
        os.makedirs(thread_path(board_name, tid), exist_ok=True)
        with open(post_file_path(board_name, tid, 'csv'), 'w', newline='') as postcsv:
            wrtr = csv.writer(postcsv)
            wrtr.writerow(['post_id', 'poster_id', 'poster_name', 'poster_email', 'poster_tripcode', 'subject', 'comment', 'html_comment', 'text_comment', 'is_op', 'timestamp', 'has_file', 'url', 'semantic_url', 'semantic_slug'])
            for post in thread.all_posts:
                wrtr.writerow([str(post.post_id), str(post.poster_id), post.name, post.email, post.tripcode, \
                    post.subject, post.comment, post.html_comment, post.text_comment, str(post.is_op), \
                    str(post.timestamp), str(post.has_file), post.url, post.semantic_url, post.semantic_slug])

        # write out attached file info to a csv
        logger.debug(FUNCTION_THREAD_UPDATE + ' writing image info to path: ' + post_images_file_path(board_name, tid, 'csv') + ' on tid: %i', tid)
        os.makedirs(thread_path(board_name, tid), exist_ok=True)
        with open(post_images_file_path(board_name, tid, 'csv'), 'w', newline='') as postcsv:
            wrtr = csv.writer(postcsv)
            wrtr.writerow(['thread_id', 'post_id', 'file_md5', 'file_md5_hex', 'filename', 'filename_original', 'file_url', 'file_extension', 'file_size', 'file_width', 'file_height', 'file_deleted', 'thumbnail_width', 'thumbnail_height', 'thumbnail_fname', 'thumbnail_url'])
            for post in thread.all_posts:
                if post.has_file:
                    img = post.file
                    wrtr.writerow([str(tid), str(post.post_id), img.file_md5, img.file_md5_hex, img.filename, img.filename_original, img.file_url, img.file_extension, str(img.file_size), str(img.file_width), str(img.file_height), str(img.file_deleted), str(img.thumbnail_width), str(img.thumbnail_height), img.thumbnail_fname, img.thumbnail_url])
            rds.sadd(IMAGE_FETCH_INDEX_KEY, post_images_file_path(board_name, tid, 'csv'))

        # reenqueue the thread if not closed / archived
        if not thread.archived and not thread.closed:
            logger.debug(FUNCTION_THREAD_UPDATE + ' reenqueueing on tid: %i', tid)
            rds.sadd(BOARD_INDEX_KEY, str(tid))
        else:
            logger.debug(FUNCTION_THREAD_UPDATE + ' not reenqueueing (closed/archived) on tid: %i', tid)

        logger.debug(FUNCTION_THREAD_UPDATE + ' run completed')
        time.sleep(task_delay)
elif process_function == FUNCTION_POST_IMAGE_FETCH:
    while 1:
        # this method pulls threads off the board liveness index and checks the thread. we update it
        # each time it's touched, until it's closed. at that time all the thread info is dumped
        logger.debug('FOURC_FUNCTION set to ' + FUNCTION_POST_IMAGE_FETCH + '; beginning execution...')

        # get the name of a post-image file that needs grabbing
        logger.debug(FUNCTION_POST_IMAGE_FETCH + ' grabbing image csv from redis')
        imgfn = rds.spop(IMAGE_FETCH_INDEX_KEY)
        if imgfn is None:
            logger.debug('popping ' + IMAGE_FETCH_INDEX_KEY + ' resulted in None fn, continuing...')
            time.sleep(task_delay)
            continue
        
        # iterate over each line in the csv, download each image listed to the thread data directory
        logger.debug(FUNCTION_POST_IMAGE_FETCH + ' operating on: ' + imgfn.decode('utf-8'))
        with open(imgfn.decode('utf-8'), 'r', newline='') as imgfile:
            try:
                imagecsv = csv.reader(imgfile)
                next(imagecsv) # skip header
                for img in imagecsv:
                    path = post_image_path(board_name, img[0], img[1], img[4])
                    if not os.path.isfile(path):
                        logger.debug(FUNCTION_POST_IMAGE_FETCH + ' fetching: ' + img[6] + ' to: ' + path)
                        try:
                            with urllib.request.urlopen(img[6]) as response, open(path, 'wb') as output:
                                shutil.copyfileobj(response, output)
                        except urllib.error.HTTPError as err:
                            logger.debug(FUNCTION_POST_IMAGE_FETCH + ' had error on: ' + img[6] + '; code: ' + str(err.code))
                        except:
                            logger.debug(FUNCTION_THREAD_UPDATE + ' encountered generic error, trying to continue')
                    else: 
                        logger.debug(FUNCTION_POST_IMAGE_FETCH + ' not fetching; file exists: ' + path)
            except IndexError as ind:
                logger.debug(FUNCTION_POST_IMAGE_FETCH + ' index error: ' + str(ind))

        logger.debug(FUNCTION_POST_IMAGE_FETCH + ' run completed')
        time.sleep(task_delay)
else:
    logger.warn('FOURC_FUNCTION env var set to unrecognized function ' + process_function)
    sys.exit(2)

