# Map Reduce

- [Map Reduce](#map-reduce)
  - [简介](#简介)
  - [概述](#概述)
  - [Map Reduce 相关代码的目录结构](#map-reduce-相关代码的目录结构)
  - [任务安排](#任务安排)

## 简介

Map Reduce: 一种可并行处理大量数据的算法模型

## 概述

- 算法流程：
  - 说明：
    - mapIndex : M 组数据文件，编号依次为 0 ~ M-1
    - partIndex : map 方法生成的每个 key 映射到唯一一个 partIndex，每个 map 任务都将生成 N 组结果文件，编号依次为 0 ~ N-1，N 为用户指定的 reduce 任务数量
  - 流程介绍:
    -> 输入待处理的数据文件 -> 将数据文件划分为 M 组 -> 将 M 组数据交给不同的 map 函数 -> 每个 map 任务输出 N 份包含 key-value pairs 的文件（文件名为 mr-mapIndex-partIndex）
    -> 待 map 任务全部处理完 
    -> 将每一组同 partIndex 的结果文件交给同一个 reduce 任务  -> 读取文件中所有 key-value pairs 并排序 -> 对每个 key 及其 values 执行一次 reduce 方法 -> 将结果迭代写入 reduce 结果文件中（文件名为 mr-out-partIndex）
    -> 待 reduce 任务全部处理完，即任务完成，输出 N 份结果文件

- 实现流程:
  - 对象说明:
    - master: 负责创建，调度任务的老板（此流程中仅一个，但实际生产环境中，需要有备份 master，以应对 master 出现故障的情况）．
    - worker: 向 master 请求任务，然后执行任务的打工人，worker 两种任务都可执行（map / reduce），具体执行什么，由 master 决定（先分配 map 任务），worker 数量众多，能力参差不齐，当 worker 任务执行失败时（近似失败），master 会将其任务分给其他的打工人执行．
  - 算法实现流程:
  -> 


## Map Reduce 相关代码的目录结构

说明: 标 * 的文件为课程自带，需先理解
```
src
│
└───main
│   │
│   |   mrmaster.go*            // master 的初始化及 server 启动（具体的流程实现请见 mr/master.go）
│   |   mrsequential.go*        // 简单的 map reduce 运行流程 （部分代码可借鉴）
│   |   mrworker.go*            // map_function & reduce_function 的加载以及 worker 主流程的调用（具体的流程实现请见 mr/worker.go) 
│   |   test-mr.sh*             // 完成后用来测试的脚本文件
│   |   pg-dorian_gray.txt*     // 要处理的数据文件
│   |   pg-being_ernest.txt*
│   |   ...
│   |   
└───mr
│   │
│   │   master.go               // Master 相关结构的定义以及与 Master 相关的方法（如任务创建，调度等），及 master rpc 服务
|   |   rpc.go                  // 任务传输所需的结构的定义，及任务队列可执行的方法的实现
|   |   worker.go               // worker 的具体工作流程，以及流程中所涉及到的各种方法（如任务请求，发送任务完成，任务执行等）
│   │
└───mrapps
|   |
│   │   crash.go*               // 带有冲突的 map & reduce 插件（设置随机超时，用于测试任务执行过程中对异常流程的解决）
|   |   indexer.go*             // 包含 key 排序的 map & reduce 插件
|   |   mtiming.go*             // 测试 map 任务是否是并行完成的插件
|   |   nocrash.go*             // 具有疑似冲突的 map & reduce 插件
│   │   rtiming.go*             // 检测 workers 是否是并行执行 reduce 任务的插件
|   |   wc.go*                  // 用于生成仅含简单计数功能的 map & reduce 插件
|   |   
```

## 任务安排