/**
 * Copyright 2013 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package datafu.mr.jobs;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import org.apache.hadoop.mapreduce.Mapper;

public class IntermediateTypeHelper {

	@SuppressWarnings("rawtypes")
	public static Type[] getMapperTypes(Class mapperClass) {
		if (!Mapper.class.isAssignableFrom(mapperClass)) {
			throw new IllegalArgumentException(
					"The input class should inherit from "
							+ Mapper.class.getName());
		}
		Type[] types = null;
		while (types == null && mapperClass != null) {
			if (mapperClass.getSuperclass() != null
					&& mapperClass.getSuperclass().equals(Mapper.class)) {
				types = ((ParameterizedType) mapperClass.getGenericSuperclass())
						.getActualTypeArguments();
				for (int i = 0; i < types.length; i++) {
					if (types[i] instanceof ParameterizedType) {
						types[i] = ((ParameterizedType) types[i]).getRawType();
					}
				}
			}
			mapperClass = mapperClass.getSuperclass();
		}
		return types;
	}

	@SuppressWarnings("rawtypes")
	public static Class getMapperOutputKeyClass(Class mapperClass) {
		return (Class) getMapperTypes(mapperClass)[2];
	}

	@SuppressWarnings("rawtypes")
	public static Class getMapperOutputValueClass(Class mapperClass) {
		return (Class) getMapperTypes(mapperClass)[3];
	}
}
