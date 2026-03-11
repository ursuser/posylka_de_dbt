{% set partitions_to_replace = [
    "date_sub(current_date, interval 1 day)",
    "date_sub(current_date, interval 2 day)",
    "date_sub(current_date, interval 3 day)",
    "date_sub(current_date, interval 4 day)",
    "date_sub(current_date, interval 5 day)",
] %}

{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={"field": "event_date", "data_type": "date"},
        partitions=partitions_to_replace,
    )
}}

with

    ga4_source as (
        select *
        from {{ source("analytics_298705553", "events") }}
        where _table_suffix >= '{{ var("ga4_start_date", "20260301") }}'

        {% if is_incremental() %}
            and parse_date('%Y%m%d', regexp_extract(_table_suffix, '[0-9]+'))
                in ({{ partitions_to_replace | join(",") }})
        {% endif %}

    ),

    prev_prep as (
        select
            user_pseudo_id,
            concat(
                user_pseudo_id,
                '_',
                (
                    select value.int_value
                    from unnest(event_params)
                    where key = "ga_session_id"
                )
            ) as session_id,
            min(event_timestamp) as session_start_at,
            min(parse_date('%Y%m%d', event_date)) as event_date,
            min_by(
                (
                    select value.string_value
                    from unnest(event_params)
                    where key = "page_location"
                ),
                event_timestamp
            ) as page_location,
            min_by(
                bqutil.fn_eu.url_parse(
                    (
                        select value.string_value
                        from unnest(event_params)
                        where key = "page_referrer"
                    ),
                    'HOST'
                ),
                event_timestamp
            ) as referrer
        from ga4_source
        where 1 = 1 and user_pseudo_id is not null
        group by 1, 2
    ),

    raw as (
        select
            * except (referrer),
            regexp_replace(referrer, r'^www\.', '') as referrer,
            net.reg_domain(page_location) as lp_domain,
            net.reg_domain(referrer) as referrer_domain
        from prev_prep
    ),

    parse_data as (
        select
            *,
            bqutil.fn_eu.url_param(page_location, 'utm_source') as source,
            bqutil.fn_eu.url_param(page_location, 'utm_medium') as medium,
            bqutil.fn_eu.url_param(page_location, 'utm_campaign') as campaign,
            bqutil.fn_eu.url_param(page_location, 'utm_term') as term,
            bqutil.fn_eu.url_param(page_location, 'utm_content') as content,
            bqutil.fn_eu.url_param(page_location, 'gclid') as gclid,
            bqutil.fn_eu.url_param(page_location, 'dclid') as dclid,
            bqutil.fn_eu.url_param(page_location, 'wbraid') as wbraid,
            bqutil.fn_eu.url_param(page_location, 'gbraid') as gbraid,
            bqutil.fn_eu.url_param(page_location, 'gclsrc') as gclsrc,
            bqutil.fn_eu.url_param(page_location, 'srsltid') as srsltid,
            bqutil.fn_eu.url_param(page_location, 'ttclid') as ttclid,
            bqutil.fn_eu.url_param(page_location, 'msclkid') as msclkid,
            bqutil.fn_eu.url_param(page_location, 'li_fat_id') as li_fat_id
        from raw
    ),

    parse_data_enriched as (
        select
            pd.*,
            gac.campaign_name as google_ads_campaign_name
        from parse_data pd
        left join {{ ref('int_gclid_campaign') }} gac on pd.gclid = gac.gclid
    ),

    sessions as (
        select
            * except (
                source,
                medium,
                campaign,
                term,
                content,
                gclid,
                dclid,
                wbraid,
                gbraid,
                srsltid,
                ttclid,
                msclkid,
                li_fat_id,
                lp_domain,
                referrer_domain
            ),
            case
                when
                    gclid is not null
                    or dclid is not null
                    or wbraid is not null
                    or gbraid is not null
                    or gclsrc is not null
                    or srsltid is not null
                then 'google'
                when source is not null
                then source
                when ttclid is not null
                then 'tiktok'
                when msclkid is not null
                then 'bing'
                when li_fat_id is not null
                then 'linkedin'
                when referrer is not null
                then
                    case
                        when lp_domain = referrer_domain
                        then null
                        when
                            regexp_contains(
                                referrer,
                                r'paypal\.|stripe\.|klarna\.|accounts\.(google|youtube)|login\.microsoftonline'
                            )
                        then null
                        when
                            regexp_contains(
                                referrer,
                                r'^google\.com$|^google\.(com?\.)?(ar|au|br|co|hk|mx|my|ng|pe|ph|pk|sa|sg|tr|tw|ua|vn|de|es|fr|gr|hu|ie|it|nl|pl|pt|ro|ru|se|uk|ae|ca|in|za|lt|be|bg|ch|cl|il|jp|kr|th|bo|kh|dk|ee|fi|gg|no|at|id|nz|uz|uy|hr|lk)$'
                            )
                        then 'google'
                        when referrer in ('alice.com', 'aliceadsl.fr')
                        then 'alice'
                        when
                            referrer
                            in ('aol.com', 'search.aol.fr', 'alicesuche.aol.de')
                        then 'aol'
                        when referrer in ('lycos.com', 'search.lycos.de')
                        then 'lycos'
                        when referrer in ('msn.com', 'money.msn.com', 'local.msn.com')
                        then 'msn'
                        when referrer in ('netscape.com', 'search.netscape.com')
                        then 'netscape'
                        when referrer in ('onet.pl', 'szukaj.onet.pl')
                        then 'onet'
                        when referrer in ('tut.by', 'search.tut.by')
                        then 'tut.by'
                        when referrer in ('ukr.net', 'search.ukr.net')
                        then 'ukr'
                        when referrer in ('yahoo.com', 'yahoo.cn')
                        then 'yahoo'
                        when referrer in ('yandex.com', 'yandex.ru')
                        then 'yandex'
                        when referrer in ('360.cn')
                        then '360.cn'
                        when referrer in ('alltheweb.com')
                        then 'alltheweb'
                        when referrer in ('altavista.com')
                        then 'altavista'
                        when referrer in ('ask.com')
                        then 'ask'
                        when referrer in ('auone.jp')
                        then 'auone'
                        when referrer in ('avg.com')
                        then 'avg'
                        when referrer in ('babylon.com')
                        then 'babylon'
                        when referrer in ('baidu.com')
                        then 'baidu'
                        when referrer in ('biglobe.ne.jp')
                        then 'biglobe'
                        when referrer in ('bing.com', 'cn.bing.com')
                        then 'bing'
                        when referrer in ('centrum.cz')
                        then 'centrum.cz'
                        when referrer in ('cnn.com')
                        then 'cnn'
                        when referrer in ('comcast.net')
                        then 'comcast'
                        when referrer in ('conduit.com')
                        then 'conduit'
                        when referrer in ('daum.net')
                        then 'daum'
                        when referrer in ('duckduckgo.com')
                        then 'duckduckgo'
                        when referrer in ('ecosia.org')
                        then 'ecosia'
                        when referrer in ('ekolay.net')
                        then 'ekolay'
                        when referrer in ('eniro.se')
                        then 'eniro'
                        when referrer in ('globo.com')
                        then 'globo'
                        when referrer in ('go.mail.ru')
                        then 'go.mail.ru'
                        when referrer in ('goo.ne.jp')
                        then 'goo.ne'
                        when referrer in ('haosou.com')
                        then 'haosou.com'
                        when referrer in ('incredimail.com')
                        then 'incredimail'
                        when referrer in ('kvasir.no')
                        then 'kvasir'
                        when referrer in ('mamma.com')
                        then 'mamma'
                        when referrer in ('mynet.com')
                        then 'mynet'
                        when referrer in ('najdi.si')
                        then 'najdi'
                        when referrer in ('naver.com')
                        then 'naver'
                        when referrer in ('ozu.es')
                        then 'ozu'
                        when referrer in ('qwant.com')
                        then 'qwant'
                        when referrer in ('rakuten.co.jp')
                        then 'rakuten'
                        when referrer in ('rambler.ru')
                        then 'rambler'
                        when referrer in ('search.smt.docomo.ne.jp')
                        then 'search.smt.docomo'
                        when referrer in ('search-results.com')
                        then 'search-results'
                        when referrer in ('sesam.no')
                        then 'sesam'
                        when referrer in ('seznam.cz')
                        then 'seznam'
                        when referrer in ('so.com')
                        then 'so.com'
                        when referrer in ('sogou.com')
                        then 'sogou'
                        when referrer in ('startsidan.se')
                        then 'startsidan'
                        when referrer in ('szukacz.pl')
                        then 'szukacz'
                        when referrer in ('terra.com.br')
                        then 'terra'
                        when referrer in ('search.virgilio.it')
                        then 'virgilio'
                        when referrer in ('voila.fr')
                        then 'voila'
                        when referrer in ('wp.pl')
                        then 'wirtualna-polska'
                        when referrer in ('yam.com')
                        then 'yam'
                        when contains_substr(referrer, 'facebook.')
                        then 'facebook'
                        when contains_substr(referrer, 'twitter.')
                        then 'twitter'
                        when contains_substr(referrer, 'linkedin.')
                        then 'linkedin'
                        when contains_substr(referrer, 'instagram.')
                        then 'instagram'
                        when contains_substr(referrer, 'youtube.')
                        then 'youtube'
                        when contains_substr(referrer, 'tiktok.')
                        then 'tiktok'
                        when contains_substr(referrer, 'reddit.')
                        then 'reddit'
                        when contains_substr(referrer, 'tumblr.')
                        then 'tumblr'
                        when regexp_contains(referrer, r'(\.|^)vk\.com$')
                        then 'vk'
                        when regexp_contains(referrer, r'(\.|^)ok\.ru$')
                        then 'ok'
                        else referrer
                    end
            end as source,
            case
                when
                    gclid is not null
                    or dclid is not null
                    or wbraid is not null
                    or gbraid is not null
                    or gclsrc is not null
                then 'cpc'
                when srsltid is not null
                then 'organic'
                when medium is not null
                then medium
                when ttclid is not null or msclkid is not null or li_fat_id is not null
                then 'cpc'
                when source is not null
                then '(none)'
                when referrer is not null
                then
                    case
                        when lp_domain = referrer_domain
                        then null
                        when
                            regexp_contains(
                                referrer,
                                r'paypal\.|stripe\.|klarna\.|accounts\.(google|youtube)|login\.microsoftonline'
                            )
                        then null
                        when
                            regexp_contains(
                                referrer,
                                r'^google\.com$|^google\.(com?\.)?(ar|au|br|co|hk|mx|my|ng|pe|ph|pk|sa|sg|tr|tw|ua|vn|de|es|fr|gr|hu|ie|it|nl|pl|pt|ro|ru|se|uk|ae|ca|in|za|lt|be|bg|ch|cl|il|jp|kr|th|bo|kh|dk|ee|fi|gg|no|at|id|nz|uz|uy|hr|lk)$'
                            )
                        then 'organic'
                        when
                            referrer in (
                                '360.cn',
                                'alice.com',
                                'aliceadsl.fr',
                                'aol.com',
                                'search.aol.fr',
                                'alicesuche.aol.de',
                                'lycos.com',
                                'search.lycos.de',
                                'msn.com',
                                'money.msn.com',
                                'local.msn.com',
                                'netscape.com',
                                'search.netscape.com',
                                'onet.pl',
                                'szukaj.onet.pl',
                                'tut.by',
                                'search.tut.by',
                                'ukr.net',
                                'search.ukr.net',
                                'yahoo.com',
                                'yahoo.cn',
                                'yahoo.com',
                                'yandex.com',
                                'yandex.ru',
                                'alltheweb.com',
                                'altavista.com',
                                'ask.com',
                                'auone.jp',
                                'avg.com',
                                'babylon.com',
                                'baidu.com',
                                'biglobe.ne.jp',
                                'bing.com',
                                'cn.bing.com',
                                'centrum.cz',
                                'cnn.com',
                                'comcast.net',
                                'conduit.com',
                                'daum.net',
                                'duckduckgo.com',
                                'ecosia.org',
                                'ekolay.net',
                                'eniro.se',
                                'globo.com',
                                'go.mail.ru',
                                'goo.ne.jp',
                                'haosou.com',
                                'incredimail.com',
                                'kvasir.no',
                                'mamma.com',
                                'mynet.com',
                                'najdi.si',
                                'naver.com',
                                'ozu.es',
                                'qwant.com',
                                'rakuten.co.jp',
                                'rambler.ru',
                                'search.smt.docomo.ne.jp',
                                'search-results.com',
                                'sesam.no',
                                'seznam.cz',
                                'so.com',
                                'sogou.com',
                                'startsidan.se',
                                'szukacz.pl',
                                'terra.com.br',
                                'search.virgilio.it',
                                'voila.fr',
                                'wp.pl',
                                'yam.com'
                            )
                        then 'organic'
                        when
                            regexp_contains(
                                referrer,
                                r'(facebook|twitter|linkedin|instagram|youtube|tiktok|reddit|tumblr)\.|(\.|^)(vk\.com|ok\.ru)$'
                            )
                        then 'social'
                        else 'referral'
                    end
            end as medium,
            case
                when campaign is not null
                then campaign
                when srsltid is not null
                then 'Shopping Free Listings'
                when
                    gclid is not null
                    or dclid is not null
                    or wbraid is not null
                    or gbraid is not null
                    or ttclid is not null
                    or msclkid is not null
                    or li_fat_id is not null
                then coalesce(google_ads_campaign_name, 'Campaign without UTM')
                when medium in ('cpc', 'ppc', 'paid')
                then coalesce(google_ads_campaign_name, 'Campaign without UTM')
                when medium is not null or source is not null
                then '(none)'
                when referrer is not null
                then
                    case
                        when lp_domain = referrer_domain
                        then null
                        when
                            regexp_contains(
                                referrer,
                                r'paypal\.|stripe\.|klarna\.|accounts\.(google|youtube)|login\.microsoftonline'
                            )
                        then null
                        when
                            regexp_contains(
                                referrer,
                                r'^google\.com$|^google\.(com?\.)?(ar|au|br|co|hk|mx|my|ng|pe|ph|pk|sa|sg|tr|tw|ua|vn|de|es|fr|gr|hu|ie|it|nl|pl|pt|ro|ru|se|uk|ae|ca|in|za|lt|be|bg|ch|cl|il|jp|kr|th|bo|kh|dk|ee|fi|gg|no|at|id|nz|uz|uy|hr|lk)$'
                            )
                        then '(organic)'
                        when
                            referrer in (
                                '360.cn',
                                'alice.com',
                                'aliceadsl.fr',
                                'aol.com',
                                'search.aol.fr',
                                'alicesuche.aol.de',
                                'lycos.com',
                                'search.lycos.de',
                                'msn.com',
                                'money.msn.com',
                                'local.msn.com',
                                'netscape.com',
                                'search.netscape.com',
                                'onet.pl',
                                'szukaj.onet.pl',
                                'tut.by',
                                'search.tut.by',
                                'ukr.net',
                                'search.ukr.net',
                                'yahoo.com',
                                'yahoo.cn',
                                'yandex.com',
                                'yandex.ru',
                                'alltheweb.com',
                                'altavista.com',
                                'ask.com',
                                'auone.jp',
                                'avg.com',
                                'babylon.com',
                                'baidu.com',
                                'biglobe.ne.jp',
                                'bing.com',
                                'cn.bing.com',
                                'centrum.cz',
                                'cnn.com',
                                'comcast.net',
                                'conduit.com',
                                'daum.net',
                                'duckduckgo.com',
                                'ecosia.org',
                                'ekolay.net',
                                'eniro.se',
                                'globo.com',
                                'go.mail.ru',
                                'goo.ne.jp',
                                'haosou.com',
                                'incredimail.com',
                                'kvasir.no',
                                'mamma.com',
                                'mynet.com',
                                'najdi.si',
                                'naver.com',
                                'ozu.es',
                                'qwant.com',
                                'rakuten.co.jp',
                                'rambler.ru',
                                'search.smt.docomo.ne.jp',
                                'search-results.com',
                                'sesam.no',
                                'seznam.cz',
                                'so.com',
                                'sogou.com',
                                'startsidan.se',
                                'szukacz.pl',
                                'terra.com.br',
                                'search.virgilio.it',
                                'voila.fr',
                                'wp.pl',
                                'yam.com'
                            )
                        then '(organic)'
                        when
                            regexp_contains(
                                referrer,
                                r'(facebook|twitter|linkedin|instagram|youtube|tiktok|reddit|tumblr)\.|(\.|^)(vk\.com|ok\.ru)$'
                            )
                        then '(social)'
                        else '(referral)'
                    end
            end as campaign,
            case
                when term is not null
                then term
                when
                    gclid is not null
                    or dclid is not null
                    or wbraid is not null
                    or gbraid is not null
                    or ttclid is not null
                    or msclkid is not null
                    or li_fat_id is not null
                then 'Term without UTM'
                when medium in ('cpc', 'ppc', 'paid')
                then 'Term without UTM'
                when referrer is not null
                then
                    case
                        when lp_domain = referrer_domain
                        then null
                        when
                            regexp_contains(
                                referrer,
                                r'paypal\.|stripe\.|klarna\.|accounts\.(google|youtube)|login\.microsoftonline'
                            )
                        then null
                        else '(none)'
                    end
            end as term,
            case
                when content is not null
                then content
                when
                    gclid is not null
                    or dclid is not null
                    or wbraid is not null
                    or gbraid is not null
                    or ttclid is not null
                    or msclkid is not null
                    or li_fat_id is not null
                then 'Content without UTM'
                when medium in ('cpc', 'ppc', 'paid')
                then 'Content without UTM'
                when referrer is not null
                then
                    case
                        when lp_domain = referrer_domain
                        then null
                        when
                            regexp_contains(
                                referrer,
                                r'paypal\.|stripe\.|klarna\.|accounts\.(google|youtube)|login\.microsoftonline'
                            )
                        then null
                        else '(none)'
                    end
            end as content,
            case
                when coalesce(gclid, dclid, wbraid, gbraid) is not null
                then coalesce(gclid, dclid, wbraid, gbraid)
                when lp_domain = referrer_domain
                then null
                when
                    regexp_contains(
                        referrer,
                        r'paypal\.|stripe\.|klarna\.|accounts\.(google|youtube)|login\.microsoftonline'
                    )
                then null
                when referrer is not null
                then '(none)'
            end as gclid
        from parse_data_enriched
    ),

    final as (
        select
            event_date,
            user_pseudo_id,
            session_id,
            ifnull(
                last_value(source ignore nulls) over (user), "(direct)"
            ) as source_lnd,
            ifnull(last_value(medium ignore nulls) over (user), "(none)") as medium_lnd,
            ifnull(
                last_value(campaign ignore nulls) over (user), "(none)"
            ) as campaign_lnd,
            ifnull(last_value(term ignore nulls) over (user), "(none)") as term_lnd,
            ifnull(
                last_value(content ignore nulls) over (user), "(none)"
            ) as content_lnd,
            ifnull(last_value(gclid ignore nulls) over (user), "(none)") as gclid_lnd,
        from sessions
        window
            user as (
                partition by user_pseudo_id
                order by session_start_at
                range between 2592000000000 preceding and current row
            )
    )

select * from final
