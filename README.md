# differential-lineage-example

这是一个使用[differential-dataflow](https://github.com/TimelyDataflow/differential-dataflow)构建的依赖查询系统,主要用来展示
`differential-dataflow`的使用方式.

`differential-dataflow`中使用`dataflow`的方式来描述业务逻辑是非常简洁的,但是目前的版本中`dataflow`和**外部世界**如何交互没有很好的文档.
本例子主要用来展示**外部世界**和`dataflow`如何进行数据交换.

## 运行
```shell
cargo run
```

## 相关讨论
- [What is the right way to read out a collection?](https://github.com/TimelyDataflow/differential-dataflow/issues/104)
- [.inspect()-ing a collection after the dataflow has been created.](https://github.com/TimelyDataflow/differential-dataflow/issues/218)