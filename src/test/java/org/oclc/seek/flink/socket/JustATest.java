/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.socket;

import org.junit.Test;

public class JustATest {
    @Test
    public void test() {
        AHello h = new Hello();
        System.out.println(h.name);
        System.out.println(h.first);
    }

    public interface IHello {
        public String name = "El Seabra";
        public String first = "El";

        public String name();
    }
    public abstract class AHello {
        public String name = "El Seabra";
        public String first = "El";
    }

    public class Hello extends AHello implements IHello {
        public String name = "Kara Seabra";
        public String first = "Kara";
        public String last = "Seabra";

        @Override
        public String name() {
            return this.name;
        }
    }
}
