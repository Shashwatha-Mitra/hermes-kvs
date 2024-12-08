import sys

sys.path.append('../src/client/')

def test(cl):
    print('Calling Put and Get')
    cl.put('Hello', 'World')
    cl.get('Hello')
