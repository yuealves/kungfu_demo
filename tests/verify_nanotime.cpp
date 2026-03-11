/**
 * verify_nanotime.cpp — 验证 libjournal.so 的 getNanoTime() 修复
 *
 * 通过 dlopen 加载 libjournal.so，调用 NanoTimer::getInstance()->getNano()，
 * 与 clock_gettime(CLOCK_REALTIME) 对比，检查偏差是否 < 1ms。
 *
 * 编译（不依赖 KungFu 头文件）：
 *   g++ -std=c++11 -O2 -o verify_nanotime verify_nanotime.cpp -ldl
 *
 * 运行：
 *   # 测试新编译的 .so
 *   LD_LIBRARY_PATH=/path/to/new/so ./verify_nanotime
 *
 *   # 对比旧 .so（可选）
 *   LD_LIBRARY_PATH=/opt/kungfu/master/lib/yijinjing ./verify_nanotime
 */

#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <time.h>
#include <dlfcn.h>

static const long NS_PER_SEC = 1000000000L;

int main()
{
    // 加载 libjournal.so
    void* handle = dlopen("libjournal.so", RTLD_NOW);
    if (!handle) {
        fprintf(stderr, "[FAIL] dlopen: %s\n", dlerror());
        fprintf(stderr, "       请设置 LD_LIBRARY_PATH 指向 libjournal.so 所在目录\n");
        return 1;
    }

    // 通过 C++ mangled name 获取 NanoTimer 方法
    //   kungfu::yijinjing::NanoTimer::getInstance()  → _ZN6kungfu9yijinjing9NanoTimer11getInstanceEv
    //   kungfu::yijinjing::NanoTimer::getNano() const → _ZNK6kungfu9yijinjing9NanoTimer7getNanoEv
    typedef void* (*GetInstanceFn)();
    typedef long (*GetNanoFn)(void*);

    GetInstanceFn getInstance = (GetInstanceFn)dlsym(handle, "_ZN6kungfu9yijinjing9NanoTimer11getInstanceEv");
    GetNanoFn getNano = (GetNanoFn)dlsym(handle, "_ZNK6kungfu9yijinjing9NanoTimer7getNanoEv");

    if (!getInstance || !getNano) {
        fprintf(stderr, "[FAIL] dlsym: %s\n", dlerror());
        dlclose(handle);
        return 1;
    }

    printf("=== getNanoTime() 精度验证 ===\n\n");

    // 先打印当前用的是哪个 .so
    Dl_info info;
    if (dladdr((void*)getInstance, &info)) {
        printf("loaded: %s\n\n", info.dli_fname);
    }

    printf("采样 20 次，每次间隔 100ms，对比 getNanoTime() 和 clock_gettime(CLOCK_REALTIME)\n\n");

    long max_abs_diff = 0;
    int fail_count = 0;

    for (int i = 0; i < 20; i++) {
        // 紧邻调用，减少测量间隔引入的误差
        void* timer = getInstance();
        long nano_time = getNano(timer);

        timespec tp;
        clock_gettime(CLOCK_REALTIME, &tp);
        long real_ns = tp.tv_sec * NS_PER_SEC + tp.tv_nsec;

        long diff = nano_time - real_ns;
        long abs_diff = labs(diff);
        if (abs_diff > max_abs_diff) max_abs_diff = abs_diff;

        const char* status = (abs_diff < 1000000) ? "OK" : "FAIL";
        if (abs_diff >= 1000000) fail_count++;

        printf("  [%2d] diff = %+10ld ns  (%+8.3f ms)  %s\n",
               i + 1, diff, diff / 1e6, status);

        struct timespec req = {0, 100000000};  // 100ms
        nanosleep(&req, NULL);
    }

    printf("\n----- 结果 -----\n");
    printf("最大绝对��差: %ld ns (%.3f ms)\n", max_abs_diff, max_abs_diff / 1e6);

    if (fail_count == 0) {
        printf("结论: PASS — 所有采样偏差 < 1ms，getNanoTime() 修复生效\n");
    } else {
        printf("结论: FAIL — %d 个采样偏差 >= 1ms，getNanoTime() 存在时钟漂移\n", fail_count);
        printf("       如果偏差在 ±500ms 范围，说明仍然是旧版 .so（整秒截断 bug）\n");
    }

    dlclose(handle);
    return (fail_count == 0) ? 0 : 1;
}
