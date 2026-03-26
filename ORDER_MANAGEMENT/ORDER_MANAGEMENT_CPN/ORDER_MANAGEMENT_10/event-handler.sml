(* generic function for writing a list of strings to .csv *)
fun write_record(file_id, l) = 
let
   val file = TextIO.openAppend(file_id)
   val _ = TextIO.output(file, list2string(l))
   val _ = TextIO.output(file, "\n")
in
   TextIO.closeOut(file)
end;

(* write event to table "event" and respective event type table *)
fun write_event(event_id, et: EventType, ea_values: string list) = 
let
	val event_file_id = "./event.csv"
	val event_type_file_id = "./event_" ^ event_map_type(et) ^ ".csv"
	val time = t2s(Mtime())
	val _ = write_record(event_file_id, [event_id, et])
	val _ = write_record(event_type_file_id, [event_id,time]^^ea_values)
in
   event_id
end;

(* write qualified relations to table "object_object" *)
fun write_relations_recursively(file, []) = TextIO.closeOut(file) | 
write_relations_recursively(file, [obj_id1, obj_id2, qualifier]::qualified_pairs) = 
   (TextIO.output(file, list2string([obj_id1, obj_id2, qualifier]));
   TextIO.output(file, "\n");
   write_relations_recursively(file, qualified_pairs));

fun cartesian_single(_,[],_) = [] | cartesian_single(obj_id1, obj_id2::obj_ids, qualifier) = [obj_id1, obj_id2, qualifier]::cartesian_single(obj_id1, obj_ids, qualifier)

fun relationship_cartesian([],_,_) = [] | relationship_cartesian(obj_id1::obj_ids, obj_ids2, qualifier) = cartesian_single(obj_id1, obj_ids2, qualifier)^^relationship_cartesian(obj_ids, obj_ids2, qualifier)

fun write_e2o_relations(qualified_pairs) =
let
   val file = TextIO.openAppend("./event_object.csv")
in
   write_relations_recursively(file, qualified_pairs)
end;

fun write_o2o_relations(qualified_pairs) =
let
   val file = TextIO.openAppend("./object_object.csv")
in
   write_relations_recursively(file, qualified_pairs)
end;

(* write object to table "object" and respective object type table *)
fun initialize_objects_recursively(object_file, object_type_file, object_type, []) = (TextIO.closeOut(object_file); TextIO.closeOut(object_type_file)) |
initialize_objects_recursively(object_file, object_type_file, object_type, (object_id::oa_values)::objects_with_attribute_values) =
let
	val ocel_time = t2s(Mtime())
	val changed_field = ""
in
   (TextIO.output(object_file, list2string([object_id, object_type]));
   TextIO.output(object_file, "\n");
   TextIO.output(object_type_file, list2string([object_id, ocel_time, changed_field]^^oa_values));
   TextIO.output(object_type_file, "\n");  
   initialize_objects_recursively(object_file, object_type_file, object_type, objects_with_attribute_values))
end;

fun initialize_objects(object_type, objects_with_attribute_values) = 
let
   val object_file_id = "./object.csv"
   val object_type_file_id = "./object_" ^ object_map_type(object_type) ^ ".csv"
   val object_file = TextIO.openAppend(object_file_id)
   val object_type_file = TextIO.openAppend(object_type_file_id)
in
   initialize_objects_recursively(object_file, object_type_file, object_type, objects_with_attribute_values)
end;

(* object type specific functions *)
fun initialize_order((oid, its, cust, price): Order) = 
let
   val objects_with_attribute_values = [oid::[Real.toString price]]
in
   initialize_objects("orders", objects_with_attribute_values)
end;

fun items_attribute_values([]) = [] | items_attribute_values((iid, (product, weight, price), cust, oid)::its: Items) = (iid::[Real.toString weight, Real.toString price])::items_attribute_values(its)

fun initialize_items(its: Items) = 
let
   val objects_with_attribute_values = items_attribute_values(its)
in
   initialize_objects("items", objects_with_attribute_values)
end;

fun initialize_package((pid, its, cust, weight, empl): Package) = 
let
   val objects_with_attribute_values = [pid::[Real.toString weight]]
in
   initialize_objects("packages", objects_with_attribute_values)
end;

fun initialize_product((pid, weight, price): ProductInfo) = 
let
   val objects_with_attribute_values = [pid::[Real.toString weight, Real.toString price]]
in
   initialize_objects("products", objects_with_attribute_values)
end;

fun initialize_customer((cId, eId1, eId2): Customer) = 
let
   val objects_with_attribute_values = [cId]::[]
in
   (
   initialize_objects("customers", objects_with_attribute_values);
   write_o2o_relations([[cId, eId1, "primarySalesRep"]]^^[[cId, eId2, "secondarySalesRep"]])
   )
end;

fun employees_attribute_values([]) = [] | employees_attribute_values((empl, role)::empls: Employees) = 
let
   val role_str = if role = Sales then "Sales" else if role = Shipment then "Shipment" else if role = Warehousing then "Warehousing" else ""
in 
   (empl::[role_str])::employees_attribute_values(empls)
end;

fun initialize_employees(empls: Employees) = 
let
   val objects_with_attribute_values = employees_attribute_values(empls)
in
   initialize_objects("employees", objects_with_attribute_values)
end;

(* Util *)

fun ints2strings([]) = [] |ints2strings(i::is) = Int.toString(i)::ints2strings(is)
fun items_to_products_relations([]) = [] | items_to_products_relations((iid, (prod, weight, price), cust, oid)::its: Items) = 
   [iid, prod, "is a"]::items_to_products_relations(its)
fun nested_reverse([]) = [] | nested_reverse(l::ls) = (rev l)::nested_reverse(ls)

(* Order Events *)

(* create order, items, customer (if not yet existent), products (if not yet existent),
   order-to-items/customer relations, event, event_PlaceOrder *)
fun write_place_order((oid, its, cust_id, price): Order) =
let
   val item_ids = fiids(its)
   val prod_ids = fprods(its)
   val event_id = "place_"^oid;
   (* e2o *)
   val event_order = [[event_id, oid, "order"]]
   val event_items = relationship_cartesian([event_id], item_ids, "item")
   val event_customer = [[event_id, cust_id, "customer"]]
   val event_products = relationship_cartesian([event_id], prod_ids, "product")
   val e2o_relations = event_order^^event_customer^^event_items^^event_products
   (* o2o *)
   val order_items = relationship_cartesian([oid], item_ids, "comprises")
   val customer_order = [[cust_id, oid, "places"]]
   val items_products = items_to_products_relations(its)
   val o2o_relations = order_items^^customer_order^^items_products
in
   (
   initialize_order((oid, its, cust_id, price));
   initialize_items(its);
   write_event(event_id, "place order", []);
   write_e2o_relations(e2o_relations);
   write_o2o_relations(o2o_relations)
   )
end;

(* This looks like a lot of redundant code (cf. "write_place_order"), but 
   one may reduce object types and introduce distinct qualifiers per event type. *)
fun write_confirm_order((oid, its, cust_id, price): Order, salesp: Employee)=
let
   val item_ids = fiids(its)
   val prod_ids = fprods(its)
   val event_id = "confirm_"^oid;
   val event_items = relationship_cartesian([event_id], item_ids, "item")
   val event_products = relationship_cartesian([event_id], prod_ids, "product")
   (* e2o *)
   val event_order = [[event_id, oid, "order"]]
   val event_customer = [[event_id, cust_id, "customer"]]
   val event_sales = [[event_id, #1 salesp, "sales person"]]
   val e2o_relations = event_order^^event_customer^^event_sales^^event_items^^event_products
in
   (
   write_event(event_id, "confirm order", []);
   write_e2o_relations(e2o_relations)
   )
end;

fun write_payment_reminder(event_counter: INT, (oid, its, cust_id, price): Order)=
let
   val item_ids = fiids(its)
   val prod_ids = fprods(its)
   val event_counter_str = Int.toString event_counter
   (* use additional counter for event id because activity occurrence is not unique per order *)
   val event_id = "reminder_"^oid^"_"^event_counter_str;
   (* e2o *)
   val event_order = [[event_id, oid, "order"]]
   val event_products = relationship_cartesian([event_id], prod_ids, "product")
   val event_items = relationship_cartesian([event_id], item_ids, "item")         
   val e2o_relations = event_order^^event_items^^event_products
in
   (
   write_event(event_id, "payment reminder", []);
   write_e2o_relations(e2o_relations)
   )
end;

fun write_pay_order((oid, its, cust_id, price): Order)=
let
   val item_ids = fiids(its)
   val prod_ids = fprods(its)
   val event_id = "pay_"^oid;
   (* e2o *)
   val event_order = [[event_id, oid, "order"]]
   val event_items = relationship_cartesian([event_id], item_ids, "item")   
   val event_products = relationship_cartesian([event_id], prod_ids, "product")
   val e2o_relations = event_order^^event_items^^event_products
in
   (
   write_event(event_id, "pay order", []);
   write_e2o_relations(e2o_relations)
   )
end;

(* Item Events *)

fun write_pick_item((iid, (prod, weight, price), cust, oid): Item, (empl, role): Employee)=
let
   val event_id = "pick_"^iid;
   (* e2o *)
   val event_item = [[event_id, iid, "item"]]
   val event_empl = [[event_id, empl, "employee"]]
   val event_prod = relationship_cartesian([event_id], [prod], "product")
   val e2o_relations = event_item^^event_empl^^event_prod
in
   (
   write_event(event_id, "pick item", []);
   write_e2o_relations(e2o_relations)
   )
end;

fun write_item_out_of_stock((iid, (prod, weight, price), cust, oid): Item, (empl, role): Employee)=
let
   val event_id = "out_of_stock_"^iid;
   (* e2o *)
   val event_item = [[event_id, iid, "item"]]
   val event_empl = [[event_id, empl, "employee"]]
   val event_prod = relationship_cartesian([event_id], [prod], "product")
   val e2o_relations = event_item^^event_empl^^event_prod
in
   (
   write_event(event_id, "item out of stock", []);
   write_e2o_relations(e2o_relations)
   )
end;

fun write_reorder_item((iid, (prod, weight, price), cust, oid): Item, (empl, role): Employee)=
let
   val event_id = "reorder_"^iid;
   (* e2o *)
   val event_item = [[event_id, iid, "item"]]
   val event_empl = [[event_id, empl, "employee"]]
   val event_prod = relationship_cartesian([event_id], [prod], "product")
   val e2o_relations = event_item^^event_empl^^event_prod
in
   (
   write_event(event_id, "reorder item", []);
   write_e2o_relations(e2o_relations)
   )
end;

(* Package Events *)

(* instantiate package object, create package-to-items/orders/customers/products relations, event, event_CreatePackage *)
(* in warehouse, a package has a packer (colset WarehousePackage). Later, only the Shipper is relevant (colset Package) *)
fun write_create_package(((pid, its, cust, weight, (shipper, role1)), (packer, role2)): WarehousePackage) =
let
   val package = (pid, its, cust, weight, (shipper, role1))
   val price = fprices(its)
   val event_id = "create_"^pid;
   val item_ids = fiids(its)
   val prod_ids = fprods(its)
   (* e2o *)
   val event_package = [[event_id, pid, "creates"]]
   val event_packer = [[event_id, packer, "packer"]]
   val event_items = relationship_cartesian([event_id], item_ids, "item")
   val event_products = relationship_cartesian([event_id], prod_ids, "product")
   val e2o_relations = event_package^^event_packer^^event_items^^event_products
   (* o2o *)
   val package_items = relationship_cartesian([pid], item_ids, "contains")
   val o2o_relations = package_items
in
   (
   initialize_package(package);
   write_event(event_id, "create package", []);
   write_e2o_relations(e2o_relations);
   write_o2o_relations(o2o_relations)
   )
end;

fun write_send_package((pid, its, cust, weight, (shipper, role1)): Package, (forwarder, role2): Employee) =
let
   val event_id = "send_"^pid;
   val item_ids = fiids(its)
   val prod_ids = fprods(its)
   (* e2o *)
   val event_package = [[event_id, pid, "shipped package"]]
   val event_shipper = [[event_id, shipper, "shipper"]]
   val event_forwarder = [[event_id, forwarder, "forwarder"]]
   val event_items = relationship_cartesian([event_id], item_ids, "item")   
   val event_products = relationship_cartesian([event_id], prod_ids, "product")
   val e2o_relations = event_package^^event_shipper^^event_forwarder^^event_items^^event_products
   (* o2o *)
   val package_shipper = [[pid, shipper, "shipped by"]]
   val o2o_relations = package_shipper
in
   (
   write_event(event_id, "send package", []);
   write_e2o_relations(e2o_relations);
   write_o2o_relations(o2o_relations)
   )
end;

fun write_failed_delivery(event_counter, (pid, its, cust, weight, (shipper, role)): Package) =
let
   val event_counter_str = Int.toString event_counter
   (* use additional counter for event id because activity occurrence is not unique per package *)
   val event_id = "fail_"^pid^"_"^event_counter_str;
   val item_ids = fiids(its)
   val prod_ids = fprods(its)
   val event_package = [[event_id, pid, "shipped package"]]
   val event_shipper = [[event_id, shipper, "shipper"]]
   val event_items = relationship_cartesian([event_id], item_ids, "item")      
   val event_products = relationship_cartesian([event_id], prod_ids, "product")   
   val e2o_relations = event_package^^event_items^^event_products^^event_shipper
in
   (
   write_event(event_id, "failed delivery", []);
   write_e2o_relations(e2o_relations)
   )
end;

fun write_package_delivered((pid, its, cust, weight, (shipper, role)): Package) =
let
   val event_id = "deliver_"^pid;
   val item_ids = fiids(its)
   val prod_ids = fprods(its)
   val event_package = [[event_id, pid, "shipped package"]]
   val event_shipper = [[event_id, shipper, "shipper"]]
   val event_items = relationship_cartesian([event_id], item_ids, "item")      
   val event_products = relationship_cartesian([event_id], prod_ids, "product")   
   val e2o_relations = event_package^^event_items^^event_products^^event_shipper
in
   (
   write_event(event_id, "package delivered", []);   
   write_e2o_relations(e2o_relations)
   )
end;

(* object attribute change *)
fun change_prices([]) = () | change_prices((prod, weight, price)::pis: ProductInfos) =
let
   val ocel_id = prod
   val ocel_time = t2s(Mtime())
   val ocel_changed_field = "price"
   val change_entry = [ocel_id, ocel_time, ocel_changed_field, "", Real.toString price]
   val object_type_file_id = "./object_" ^ object_map_type("products") ^ ".csv"
in
   (write_record(object_type_file_id, change_entry); change_prices(pis))
end;
