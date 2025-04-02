//
// Created by ruihong on 4/20/23.
//
#include "storage/DSMEngine/mutexlock.h"
thread_local bool DSMEngine::SpinLock::owns = false;
