#!/usr/bin/env python
import sys
import unittest

if __name__ == '__main__':
    # Discover and run tests
    loader = unittest.TestLoader()
    start_dir = 'tests'
    suite = loader.discover(start_dir, pattern='test_*.py')
    
    # Run tests with verbosity=2 for detailed output
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Exit with non-zero code if tests failed
    sys.exit(not result.wasSuccessful())