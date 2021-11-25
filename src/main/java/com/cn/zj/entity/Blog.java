package com.cn.zj.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Description:
 * @Author: wangdakai
 * @Date: 2021/11/25
 */
@Data
@AllArgsConstructor
public class Blog {
    private int id;
    private String title;
    private String content;
}
