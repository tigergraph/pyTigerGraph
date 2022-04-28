from dataclasses import dataclass
from enum import Enum
from typing import Optional, List

ArticleIdMap = dict
CustomerIdMap = dict


class UserColumn(Enum):
    PostalCode = "postal_code"
    FN = "FN"
    Age = "age"
    ClubMemberStatus = "club_member_status"
    FashionNewsFrequency = "fashion_news_frequency"
    Active = "Active"


class ArticleColumn(Enum):
    ProductCode = "product_code"
    ProductTypeNo = "product_type_no"
    GraphicalAppearanceNo = "graphical_appearance_no"
    ColourGroupCode = "colour_group_code"
    AvgPrice = "avg_price"
    ImgEmbedding = "img_embedding"


@dataclass
class DataLoaderConfig:
    val_split: float
    test_split: float


@dataclass
class PreprocessingConfig:
    customer_features: List[UserColumn]
    # customer_nodes: List[UserColumn]

    article_features: List[ArticleColumn]
    # article_nodes: List[ArticleColumn]

    article_non_categorical_features: List[ArticleColumn]

    K: int
    data_size: Optional[int]
