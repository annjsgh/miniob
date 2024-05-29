/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by wangyunlai on 2021/6/11.
//

#include <string.h>
#include <algorithm>
#include "common/defs.h"

namespace common {


int compare_int(void *arg1, void *arg2)
{
  int v1 = *(int *)arg1;
  int v2 = *(int *)arg2;
  return v1 - v2;
}

int compare_float(void *arg1, void *arg2)
{
  float v1 = *(float *)arg1;
  float v2 = *(float *)arg2;
  float cmp = v1 - v2;
  if (cmp > EPSILON) {
    return 1;
  }
  if (cmp < -EPSILON) {
    return -1;
  }
  return 0;
}

int compare_string(void *arg1, int arg1_max_length, void *arg2, int arg2_max_length)
{
  const char *s1 = (const char *)arg1;
  const char *s2 = (const char *)arg2;
  int maxlen = std::min(arg1_max_length, arg2_max_length);
  int result = strncmp(s1, s2, maxlen);
  if (0 != result) {
    return result;
  }

  if (arg1_max_length > maxlen) {
    return s1[maxlen] - 0;
  }

  if (arg2_max_length > maxlen) {
    return 0 - s2[maxlen];
  }
  return 0;
}

int compare_date(void* arg1, void* arg2){
  return compare_int(arg1, arg2);
}

int compare_str_with_int(void *arg1, int arg1_max_length, void *arg2) {
  int temp2 = *(int *)arg2;
  const char *s1 = (const char *)arg1;
  if (s1[0]>'9'||s1[0]<'0'){ //start with invalid char , set zero
    return (0-temp2);
  }else { // start with number
    int temp1=0.0;
    int place=1;
    for(int a=0;a<arg1_max_length;a++){
      if (s1[a]<='9'&& s1[a]>='0'){
        float temp =s1[a]-'0';
          temp1*=place;
          temp1+=temp;
      }else{
        break;
      }
      place*=10;
    }
    return temp1-temp2;
  }
}

int compare_str_with_float(void *arg1, int arg1_max_length, void *arg2) {
  float temp2 = *(float *)arg2;
  const char *s1 = (const char *)arg1;
  if (s1[0]>'9'||s1[0]<'0'){ //start with invalid char , set zero
    return (0.0-temp2);
  }else{ // start with number
    float temp1=0.0;
    float place=1.0;
    bool flag=true;
    for(int a=0;a<arg1_max_length;a++){
      if (s1[a]<='9'&& s1[a]>='0'){
        float temp =s1[a]-'0';
        if(flag){
          temp1*=place;
          temp1+=temp;
        }else{
          temp*=place;
          temp1+=temp;
        }
      }else if(s1[a]=='.'){
        place=1;
        flag=false;
      }else{
        break;
      }
      if(flag){
        place*=10.0;
      }else{
        place*=0.1;
      }
    }
    float cmp = temp1 - temp2;
  if (cmp > EPSILON) {
    return 1;
  }
  if (cmp < -EPSILON) {
    return -1;
  }
  return 0;
  }
}

 }// namespace common
