package com.jayden.estest;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author Jayden Sun（089245）
 * @since 2020/1/6
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Item {
    private String id;
    private String title; //标题

    private String category;// 分类

    private String brand; // 品牌

    private Double price; // 价格

    private String images; // 图片地址

    private String images2; // 图片地址2

    private List<ItemSub> counts;


}
