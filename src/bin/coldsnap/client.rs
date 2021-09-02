/*!
Provides a macro to create rusoto clients of various types, using optional region, endpoint, and
profile information from the user.  We don't expose the full range of configuration options but
this should cover most scenarios.
*/

/// Create a rusoto client of the given type using the (optional) given region, endpoint, and credentials.
macro_rules! build_client {
    ($client_type:ty, $region_name:expr, $endpoint:expr, $profile:expr) => {{
	let mut http_config_with_bigger_buffer = HttpConfig::new();
	http_config_with_bigger_buffer.read_buf_size(520 * 1024); // 512K chunk + some overhead
        let http_client = HttpClient::new_with_config(http_config_with_bigger_buffer).context(error::CreateHttpClient)?;
        let profile_provider = match $profile {
            Some(profile) => {
                let mut p = ProfileProvider::new().context(error::CreateProfileProvider)?;
                p.set_profile(profile);
                Some(p)
            }
            None => None,
        };

        let profile_region = profile_provider
            .as_ref()
            .and_then(|p| p.region_from_profile().ok().flatten());

        fn parse_region(region: Option<String>) -> Result<Option<Region>> {
            region
                .map(|r| r.parse().context(error::ParseRegion { region: r }))
                .transpose()
        }

        let region = parse_region($region_name)?.or(parse_region(profile_region)?);

        let region = match (region, $endpoint) {
            (Some(region), Some(endpoint)) => Region::Custom {
                name: region.name().to_string(),
                endpoint,
            },
            (Some(region), None) => region,
            (None, Some(endpoint)) => Region::Custom {
                name: Region::default().name().to_string(),
                endpoint,
            },
            (None, None) => Region::default(),
        };

        match profile_provider {
            Some(provider) => Ok(<$client_type>::new_with(http_client, provider, region)),
            None => Ok(<$client_type>::new_with(
                http_client,
                ChainProvider::new(),
                region,
            )),
        }
    }};
}
