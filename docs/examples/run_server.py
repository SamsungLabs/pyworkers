
if __name__ == '__main__':
    import sys
    sys.argv[1:] = ['-v', '--addr', '127.0.0.1', '--port', '60006']
    import runpy
    runpy.run_module('pyworkers.remote_server', run_name='__main__', alter_sys=True)
