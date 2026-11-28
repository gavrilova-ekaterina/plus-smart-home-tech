package ru.yandex.practicum.product.service;

import ru.yandex.practicum.product.dto.*;

import java.util.List;

public interface ProductService {

    List<ProductDto> getAllProducts();

    ProductDto getProductById(Long id);

    ProductDto createProduct(CreateProductRequest request);

    ProductDto updateProduct(Long id, UpdateProductRequest request);

    List<ProductDto> searchProducts(String query);

    List<ProductDto> getProductsByCategory(Long categoryId);

    List<CategoryDto> getAllCategories();

    CategoryDto getCategoryById(Long id);

    CategoryDto createCategory(CreateCategoryRequest request);
}
