package secondlc;

import org.hibernate.cache.infinispan.InfinispanRegionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
public class InfinispanStalePutFromLoadCacheTest
      extends BaseStalePutFromLoadCacheTest {

   @Override
   void configureCache(Configuration cfg) {
      cfg.setProperty(Environment.CACHE_REGION_FACTORY,
            InfinispanRegionFactory.class.getName());
      cfg.setProperty(Environment.JTA_PLATFORM,
            "org.hibernate.service.jta.platform.internal.JBossStandAloneJtaPlatform");
      cfg.setProperty(InfinispanRegionFactory.INFINISPAN_CONFIG_RESOURCE_PROP,
            "2lc-infinispan.xml");
   }

}
