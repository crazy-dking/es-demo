package com.cn.zj.service;

import com.cn.zj.entity.Blog;
import com.cn.zj.service.impl.EsServiceImpl;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @Description:
 * @Author: wangdakai
 * @Date: 2021/11/25
 */
public class EsServiceTest {
    private EsServiceImpl esService;
    @Before
    public void beforeTest(){
        esService = new EsServiceImpl();
    }
    @Test
    public void getScrollTest() throws IOException {
        Map<String, Object> map = esService.searchPageScroll("来", "DnF1ZXJ5VGhlbkZldGNoAgAAAAAAAADUFm9uUHJfN2hOUl82NTdWa2E5RWdtU3cAAAAAAAAA0xZvblByXzdoTlJfNjU3VmthOUVnbVN3", 1);
        System.out.println(map.get("scrollId"));
        System.out.println(map.get("list").toString());

    }
    @Test
    public void getListPage() throws IOException {
        List<Blog> blogs = esService.searchByPage("来", 2, 1);
        System.out.println(blogs.toString());
    }
    @Test
    public void getList() throws IOException {
        List<Blog> blogs = esService.searchByKeywords("来");
        System.out.println(blogs.toString());
    }

    @Test
    public void deleteTest() throws IOException {
        esService.deleteById(1);
    }

    @Test
    public void updateTest() throws IOException {
        Blog blog = new Blog(1, "jbw1", "jbw1又出来了");
        esService.update(blog);
    }
    @Test
    public void getTest() throws IOException {
        Blog blogById = esService.getBlogById(1);
        System.out.println(blogById.toString());
    }
    @Test
    public void addTest() throws IOException {
        Blog blog = new Blog(2, "jbw2", "jbw2又出来了");
        esService.add(blog);
    }
}
