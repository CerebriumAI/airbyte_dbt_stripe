select
    _airbyte_charges_hashid as charges_hashid,
    brand as card_brand,
    funding as card_funding,
    country as card_country
from {{ var('charges_card')}}