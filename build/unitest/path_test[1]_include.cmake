if(EXISTS "/Users/caijiyan/Desktop/CJYsama/senior_lesson/db_senior/db_impl_course/build/unitest/path_test[1]_tests.cmake")
  include("/Users/caijiyan/Desktop/CJYsama/senior_lesson/db_senior/db_impl_course/build/unitest/path_test[1]_tests.cmake")
else()
  add_test(path_test_NOT_BUILT path_test_NOT_BUILT)
endif()
