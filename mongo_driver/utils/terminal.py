from contextlib import contextmanager


class Color(object):
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


@contextmanager
def color_terminal(color):
    def print_f(p_str):
        if color is not None:
            print('%s%s%s' % (color, p_str, Color.ENDC))
        else:
            print(p_str)
    yield print_f
