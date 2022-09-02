alter table region add constraint region_c1 primary key (r_regionkey) disable novalidate;
alter table nation add constraint nation_c1 primary key (n_nationkey) disable novalidate;
alter table part add constraint part_c1 primary key (p_partkey) disable novalidate;
alter table supplier add constraint supplier_c1 primary key (s_suppkey) disable novalidate;
alter table partsupp add constraint partsupp_c1 primary key (ps_partkey, ps_suppkey) disable novalidate;
alter table customer add constraint customer_c1 primary key (c_custkey) disable novalidate;
alter table lineitem add constraint lineitem_c1 primary key (l_orderkey, l_linenumber) disable novalidate;
alter table orders add constraint orders_c1 primary key (o_orderkey) disable novalidate;

alter table nation add constraint nation_c2 foreign key (n_regionkey) references region(r_regionkey) disable novalidate rely;
alter table supplier add constraint supplier_c2 foreign key (s_nationkey) references nation(n_nationkey) disable novalidate rely;
alter table customer add constraint customer_c2 foreign key (c_nationkey) references nation(n_nationkey) disable novalidate rely;
alter table partsupp add constraint partsupp_c2 foreign key (ps_suppkey) references supplier(s_suppkey) disable novalidate rely;
alter table partsupp add constraint partsupp_c3 foreign key (ps_partkey) references part(p_partkey) disable novalidate rely;
alter table orders add constraint orders_c2 foreign key (o_custkey) references customer(c_custkey) disable novalidate rely;
alter table lineitem add constraint lineitem_c2 foreign key (l_orderkey) references orders(o_orderkey) disable novalidate rely;
alter table lineitem add constraint lineitem_c3 foreign key (l_partkey, l_suppkey) references partsupp(ps_partkey, ps_suppkey) disable novalidate rely;
