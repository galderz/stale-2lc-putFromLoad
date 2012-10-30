package secondlc;

import org.hibernate.cache.ehcache.EhCacheRegionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
public class EhCacheStalePutFromLoadCacheTest
      extends BaseStalePutFromLoadCacheTest {

   @Override
   void configureCache(Configuration cfg) {
      cfg.setProperty(Environment.CACHE_REGION_FACTORY,
            EhCacheRegionFactory.class.getName());
      cfg.setProperty(Environment.CACHE_PROVIDER_CONFIG,
            "2lc-ehcache.xml");
   }

}
