/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.serialization;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.handle.DefaultHandle;
import org.apache.flume.serialization.handle.HandleLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.zip.GZIPInputStream;

/**
 * @author : LiuWeidong
 * @date : 2019/5/19 14:34
 * @describe : 实现Sink端解压Gz Events逻辑
 * ` a1.sinks.k1.sink.serializer = org.apache.flume.serialization.BodyGZipEventSerializer$Builder `
 */
public class BodyGZipEventSerializer implements EventSerializer {

  private static final Logger logger = LoggerFactory.getLogger(BodyGZipEventSerializer.class);

  // 配置名
  private static final String HANDLE_LINE_KEY = "handleLine";
  // handleLine 的默认配置值
  private static final String HANDLE_LINE_DEFAULT_VAL = DefaultHandle.class.getCanonicalName();

  private final HandleLine handle;
  private final OutputStream out;

  @SuppressWarnings("unchecked")
  private BodyGZipEventSerializer(Context ctx, OutputStream out) {
    // 获取配置 a1.sinks.k1.sink.serializer.handleLine 的参数
    // 默认为 org.apache.flume.serialization.handle.DefaultHandle$Builder
    String className = ctx.getString(HANDLE_LINE_KEY, HANDLE_LINE_DEFAULT_VAL);
    HandleLine.Builder builder = null;
    try {
      Class builderClass = Class.forName(className);
      if (builderClass != null) {
        builder = ((Class<? extends HandleLine.Builder>) builderClass).newInstance();
      }
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      logger.error("Can not handle class :" + className, e);
    }

    if (builder == null) {
      // builder创建失败 使用默认处理类DefaultHandle
      this.handle = new DefaultHandle.Builder().build(ctx);
      logger.warn("Can not handle class : " + className
          + ", use default handle class : "
          + "org.apache.flume.serialization.handle.DefaultHandle$Builder");
    } else {
      // 根据配置创建自定义的处理类
      this.handle = builder.build(ctx);
      logger.info("Use custom class to handle : " + className);
    }

    this.out = out;
  }

  @Override
  public void write(Event event) throws IOException {
    // 获取GZ输入流
    GZIPInputStream stream = new GZIPInputStream(new ByteArrayInputStream(event.getBody()));
    // 获取缓冲流
    BufferedReader br = new BufferedReader(new InputStreamReader(stream));

    String line;
    // 遍历每一行 调用HandleLine的实现类对每一行进行处理 最后输出到HDFS
    while ((line = br.readLine()) != null) {
      String result = this.handle.solve(line);
      byte[] bytes = result.getBytes();
      this.out.write(bytes);
    }

    br.close();
    stream.close();
  }

  @Override
  public boolean supportsReopen() {
    return true;
  }

  @Override
  public void afterCreate() throws IOException {
    // noop
  }

  @Override
  public void afterReopen() throws IOException {
    // noop
  }

  @Override
  public void beforeClose() throws IOException {
    // noop
  }

  @Override
  public void flush() throws IOException {
    // noop
  }

  public static class Builder implements EventSerializer.Builder {

    @Override
    public EventSerializer build(Context context, OutputStream out) {
      return new BodyGZipEventSerializer(context, out);
    }
  }
}
