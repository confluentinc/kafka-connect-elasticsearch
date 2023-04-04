/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.elasticsearch_2_4.util;

import static java.lang.String.format;

public final class ClassHelper {

  @SuppressWarnings("unchecked")
  public static <T> T createInstance(
          final String configKey,
          final String className,
          final Class<T> clazz) {
    return createInstance(
      configKey,
      className,
      clazz,
      () -> (T) Class.forName(className).getConstructor().newInstance());
  }

  public static <T> T createInstance(
      final String configKey,
      final String className,
      final Class<T> clazz,
      final ClassCreator<T> cc) {
    try {
      return cc.init();
    } catch (ClassCastException e) {
      throw new IllegalStateException(
        format("Contract violation %s class doesn't implement: '%s'",
        className,
       clazz.getSimpleName())
      );
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(
        format("Class %s not found: %s",
        className,
        e.getMessage())
      );
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(
        format("Class %s could not be initialized, no public default constructor: %s",
        className,
        e.getMessage())
      );
    } catch (Exception e) {
      throw new IllegalStateException(
        format("Unhandled exception was thrown while creating %s class instance with message: '%s'",
        className,
        e.getMessage())
      );
    }
  }




  @FunctionalInterface

  public interface ClassCreator<T> {

    T init() throws Exception;

  }




  private ClassHelper() {}

}
