drop sequence menuVersion;
create sequence menuVersion START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;

create or replace procedure create_json_menu
is
    fHandle         UTL_FILE.FILE_TYPE;
    -- CURSORS FOR THE DIFFERENT LOOPS
    CURSOR selection_groups_cur     IS SELECT * FROM SELECTION_GROUPS;
    selection_groups_rec            selection_groups_cur%ROWTYPE;

    CURSOR selections_cur(cp_sg number)   IS SELECT sel.sel_id, sel.name, sel.description FROM SELECTIONS sel JOIN SELGS_SELS sgs ON sgs.sel_id = sel.sel_id WHERE sgs.selg_id = cp_sg;
    selections_rec                  selections_cur%ROWTYPE;

    CURSOR items_cur                IS SELECT it.item_id, REPLACE(it.name, '"', '\"') as NAME, NVL(it.subtitle,'') "SUBTITLE", NVL(it.description,'') "DESCRIPTION", it.allergenes FROM ITEMS it ORDER BY it.item_id;
    items_rec                       items_cur%ROWTYPE;

    CURSOR item_prices_cur(cp_iid number)      IS SELECT * FROM item_prices WHERE item_id = cp_iid ORDER BY price;
    item_prices_rec                 item_prices_cur%ROWTYPE;

    CURSOR categories_cur           IS SELECT c.cat_id, c.name, c.description, c.lft, c.rgt, c.weight FROM nested_categories c;
    categories_rec                  categories_cur%ROWTYPE;

    CURSOR menus_cur                IS select m.menu_id, m.name, m.description, mt.mt_id, mt.json_tag from menus m join menu_types mt on mt.mt_id = m.mt_id;
    menus_rec                       menus_cur%ROWTYPE;

    CURSOR w_menus_cur(cp_mid number)          IS select menu_id, weight, item_id from w_menus_prices WHERE menu_id = cp_mid;
    w_menus_rec                     w_menus_cur%ROWTYPE;

    CURSOR menucat_cur              IS SELECT menu_id FROM MENUS;
    menucat_rec                     menucat_cur%ROWTYPE;

    CURSOR menucat_cat_cur          IS SELECT menu_id, cat_id from nested_categories;
    menucat_cat_rec                 menucat_cat_cur%ROWTYPE;

    CURSOR catitems_cur       IS SELECT cat_id from nested_categories order by cat_id;
    catitems_rec            catitems_cur%ROWTYPE;

    CURSOR catitems_items_cur(cp_cat_id number)       IS SELECT item_id FROM items where cat_id = cp_cat_id order by item_id;
    catitems_items_rec      catitems_items_cur%ROWTYPE;

    CURSOR sgitems_cur              IS SELECT distinct(it.item_id) FROM items it join items_selgs isg on isg.item_id = it.item_id order by it.item_id;
    sgitems_rec             sgitems_cur%ROWTYPE;

    CURSOR sgitems_sg_cur           IS SELECT isg.selg_id, isg.item_id FROM selection_groups sg join items_selgs isg on isg.selg_id = sg.selg_id;
    sgitems_sg_rec          sgitems_sg_cur%ROWTYPE;

    --THE CLOB THAT GETS THE WHOLE FILE
    res         CLOB;

    --THE JSON_ARRAY_T THAT WILL CONTAIN ALL THE DATA
    jo_result   JSON_ARRAY_T;

    --USED TO CREATE META_DATA
    var_metadata_object             JSON_OBJECT_T;
    var_metadata_array              JSON_ARRAY_T;

    --USED TO CREATE SELECTIONGROUPS
    var_sg_def_object               JSON_OBJECT_T;
    var_sg_contents_array           JSON_ARRAY_T;
    var_sg_object                   JSON_OBJECT_T;
    var_sg_sel_contents_array       JSON_ARRAY_T;
    var_sg_sel_object               JSON_OBJECT_T;

    --USED TO CREATE ITEMDEFINITIONS
    var_itd_def_object              JSON_OBJECT_T;
    var_itd_contents_array          JSON_ARRAY_T;
    var_itd_item_object             JSON_OBJECT_T;
    var_itd_item_prices_array       JSON_ARRAY_T;
    var_itd_item_price_object       JSON_OBJECT_T;

    --USED TO CREATE CATEGORYDEFINITIONS
    var_cd_def_object               JSON_OBJECT_T;
    var_cd_contents_array           JSON_ARRAY_T;
    var_cd_cat_object               JSON_OBJECT_T;


    --USED TO CREATE MENUDEFINITIONS
    var_men_def_object              JSON_OBJECT_T;
    var_men_contents_array          JSON_ARRAY_T;
    var_men_menu_object             JSON_OBJECT_T;
    var_men_menu_contents_array     JSON_ARRAY_T;
    var_men_menu_content_object     JSON_OBJECT_T;

    --USED TO CREATE STARTDEFINITIONS
    var_mc_def_object               JSON_OBJECT_T;
    var_mc_contents_array           JSON_ARRAY_T;
    var_mc_content_object           JSON_OBJECT_T;
    var_mc_content_cat_array        JSON_ARRAY_T;

    --USED TO CREATE CATEGORY ITEMS
    var_ci_def_object               JSON_OBJECT_T;
    var_ci_contents_array           JSON_ARRAY_T;
    var_ci_content_object           JSON_OBJECT_T;
    var_ci_content_item_array       JSON_ARRAY_T;

    --USED  TO CONNECT ITEMS AND SGROUPS
    var_isg_def_object               JSON_OBJECT_T;
    var_isg_contents_array           JSON_ARRAY_T;
    var_isg_content_object           JSON_OBJECT_T;
    var_isg_content_item_array       JSON_ARRAY_T;

    var_json_element                JSON_ELEMENT_T;

    cur_timestamp TIMESTAMP;

    CURSOR awaiting_cur             IS SELECT filename, content FROM awaiting_table;
    awaiting_rec                    awaiting_cur%ROWTYPE;

begin
    LOCK TABLE awaiting_table IN EXCLUSIVE MODE;

    --Initialisation of the Resulting
    jo_result := new JSON_ARRAY_T();

    --Generate the MetaData
    var_metadata_object := new JSON_OBJECT_T();
    var_metadata_object.put('type','META-INFO');
    var_metadata_object.put('version',to_char(sysdate, 'YYYY') || '.' || menuVersion.nextVal);
    var_metadata_array := new JSON_ARRAY_T('["en"]');
    var_metadata_object.put('acceptedLanguages', var_metadata_array);
    var_metadata_object.put('standardLanguage', 'en');
    var_metadata_object.put('creationDate', to_char(sysdate,'YYYY-MM-DD HH24:MI:SS'));

    --Append the Meta Data
    jo_result.append(var_metadata_object);

    --START OF SELECTIONGROUPDEFINITIONS
    var_sg_def_object := new JSON_OBJECT_T();
    var_sg_def_object.put('type','SelectionGroupDefinitions');

    --Contents
    var_sg_contents_array := new JSON_ARRAY_T();

    --Fetch the different selection_groups
    open selection_groups_cur;
    loop
        fetch selection_groups_cur into selection_groups_rec;
        exit when selection_groups_cur%NOTFOUND;

        var_sg_object := new JSON_OBJECT_T();
        var_sg_object.put('type','selectionGroup');
        var_sg_object.put('id', selection_groups_rec.selg_id);
        var_sg_object.put('names', new JSON_ARRAY_T('[{"language":"en", "value":"' || selection_groups_rec.name ||'"}]'));

        var_sg_sel_contents_array := new JSON_ARRAY_T();

        --fetch the corresponding selections
        open selections_cur(selection_groups_rec.selg_id);
        loop
            fetch selections_cur into selections_rec;
            exit when selections_cur%NOTFOUND;

            var_sg_sel_object := new JSON_OBJECT_T();
            var_sg_sel_object.put('type','selection');
            var_sg_sel_object.put('id', selections_rec.sel_id);
            var_sg_sel_object.put('names', new JSON_ARRAY_T('[{"language":"en", "value":"' || selections_rec.name ||'"}]'));
            var_sg_sel_object.put('descriptions', new JSON_ARRAY_T('[{"language":"en", "value":"' || selections_rec.description ||'"}]'));

            --Add the Selection to the selection array
            var_sg_sel_contents_array.append(var_sg_sel_object);

        end loop;
        close selections_cur;

        --add the selection array to the contents of the current selection_group
        var_sg_object.put('contents', var_sg_sel_contents_array);

        --add the current selection group to the group definitions contents
        var_sg_contents_array.append(var_sg_object);

    end loop;
    --add the contents to the group definitions
    var_sg_def_object.put('contents',var_sg_contents_array);

    --append the selection groups to the result
    jo_result.append(var_sg_def_object);

    --START OF ITEMDEFINITIONS
    var_itd_def_object := new JSON_OBJECT_T();
    var_itd_def_object.put('type', 'ItemDefinitions');

    --Contents
    var_itd_contents_array := new JSON_ARRAY_T();

    --Fetch the different Items
    open items_cur;
    loop
        fetch items_cur into items_rec;
        exit when items_cur%NOTFOUND;

        var_itd_item_object := new JSON_OBJECT_T();
        var_itd_item_object.put('type','item');
        var_itd_item_object.put('id', items_rec.item_id);
        var_itd_item_object.put('names', new JSON_ARRAY_T('[{"language":"en", "value":"' || items_rec.name || '"}]'));
        var_itd_item_object.put('subtitles', new JSON_ARRAY_T('[{"language":"en", "value":"' || items_rec.subtitle || '"}]'));
        var_itd_item_object.put('descriptions', new JSON_ARRAY_T('[{"language":"en", "value":"' || items_rec.description ||'"}]'));

        var_itd_item_prices_array := new JSON_ARRAY_T();

        open item_prices_cur(items_rec.item_id);
        loop
            fetch item_prices_cur into item_prices_rec;
            exit when item_prices_cur%NOTFOUND;

            var_itd_item_price_object := new JSON_OBJECT_T();
            if item_prices_rec.price_name is not null then
                var_itd_item_price_object.put('names', new JSON_ARRAY_T('[{"language":"en", "value":"' || item_prices_rec.price_name || '"}]'));
            end if;

            var_itd_item_price_object.put('price', item_prices_rec.price);
                --Add Prices to the Price Array
                var_itd_item_prices_array.append(var_itd_item_price_object);
        end loop;
        close item_prices_cur;

        --only add it if an element exists.
        if var_itd_item_prices_array.get_size() > 0 then
            var_itd_item_object.put('prices', var_itd_item_prices_array);
        end if;

        var_itd_item_object.put('allergenes', items_rec.allergenes);

        var_itd_contents_array.append(var_itd_item_object);
    end loop;

    var_itd_def_object.put('contents', var_itd_contents_array);
    jo_result.append(var_itd_def_object);


    --START CATEGORY DEFINITIONS
    var_cd_def_object := new JSON_OBJECT_T();
    var_cd_def_object.put('type', 'CategoryDefinitions');

    --Contents
    var_cd_contents_array := new JSON_ARRAY_T();

    --Fetch the different categories
    open categories_cur;
    loop
        fetch categories_cur into categories_rec;
        exit when categories_cur%NOTFOUND;

        var_cd_cat_object := new JSON_OBJECT_T();

        if categories_rec.weight > 0 then --check if it is a customizablecategory
            var_cd_cat_object.put('type','customizableCategory');
            var_cd_cat_object.put('weight', categories_rec.weight);
        else
            var_cd_cat_object.put('type','category');
        end if;
        var_cd_cat_object.put('id', categories_rec.cat_id);
        var_cd_cat_object.put('names', new JSON_ARRAY_T('[{"language":"en", "value":"' || categories_rec.name || '"}]'));
        var_cd_cat_object.put('descriptions', new JSON_ARRAY_T('[{"language":"en", "value":"' || categories_rec.description ||'"}]'));
        var_cd_contents_array.append(var_cd_cat_object);
    end loop;

    var_cd_def_object.put('contents',var_cd_contents_array);
    jo_result.append(var_cd_def_object);

    --START MENU DEFINITIONS
    var_men_def_object := new JSON_OBJECT_T();
    var_men_def_object.put('type','MenuDefinitions');

    --Contents
    var_men_contents_array := new JSON_ARRAY_T();

    --fetch the different menus
    open menus_cur;
    loop
        fetch menus_cur into menus_rec;
        exit when menus_cur%NOTFOUND;

        var_men_menu_object := new JSON_OBJECT_T();
        var_men_menu_object.put('type', menus_rec.json_tag);
        var_men_menu_object.put('id', menus_rec.menu_id);

        var_men_menu_object.put('names', JSON_ARRAY_T('[{"language":"en","value":"' || menus_rec.name || '"}]'));
        var_men_menu_object.put('descriptions', JSON_ARRAY_T('[{"language":"en","value":"' || menus_rec.description || '"}]'));

        var_men_menu_contents_array := new JSON_ARRAY_T();

        open w_menus_cur(menus_rec.menu_id);
        loop
            fetch w_menus_cur into w_menus_rec;
            exit when w_menus_cur%NOTFOUND;

            var_men_menu_content_object := new JSON_OBJECT_T();
            var_men_menu_content_object.put('id', w_menus_rec.item_id);
            var_men_menu_content_object.put('weight', w_menus_rec.weight);

            var_men_menu_contents_array.append(var_men_menu_content_object);

        end loop;
        close w_menus_cur;

        if var_men_menu_contents_array.get_size() > 0 then
            var_men_menu_object.put('contents', var_men_menu_contents_array);
        end if;

        var_men_contents_array.append(var_men_menu_object);

    end loop;

    var_men_def_object.put('contents', var_men_contents_array);

    jo_result.append(var_men_def_object);

    -- START MENU CONTENT DEFINITION

    var_mc_def_object := new JSON_OBJECT_T();
    var_mc_def_object.put('type', 'MenuContentDefinition');

    var_mc_contents_array := new JSON_ARRAY_T();

    open menucat_cur;
    loop
        fetch menucat_cur into menucat_rec;
        exit when menucat_cur%NOTFOUND;

        var_mc_content_object := new JSON_OBJECT_T();
        var_mc_content_object.put('menuID', menucat_rec.menu_id);

        var_mc_content_cat_array := new JSON_ARRAY_T();

        open menucat_cat_cur;
        loop
            fetch menucat_cat_cur into menucat_cat_rec;
            exit when menucat_cat_cur%NOTFOUND;

            if menucat_cat_rec.menu_id = menucat_rec.menu_id then
                var_mc_content_cat_array.append(menucat_cat_rec.cat_id);
            end if;

        end loop;
        close menucat_cat_cur;
        var_mc_content_object.put('contents', var_mc_content_cat_array);

        var_mc_contents_array.append(var_mc_content_object);

    end loop;

    var_mc_def_object.put('contents', var_mc_contents_array);
    jo_result.append(var_mc_def_object);

    --START CATEGORY CATEGORY CONTENT DEFINTITION (NOT YET IMPORTANT)

    --START CATEGORY ITEM CONTENT DEFINITION

    var_ci_def_object := new JSON_OBJECT_T();
    var_ci_def_object.put('type', 'CategoryItemContentDefinition');

    var_ci_contents_array := new JSON_ARRAY_T();

    open catitems_cur;
    loop
        fetch catitems_cur into catitems_rec;
        exit when catitems_cur%NOTFOUND;

        var_ci_content_object := new JSON_OBJECT_T();
        var_ci_content_object.put('categoryID', catitems_rec.cat_id);

        var_ci_content_item_array := new JSON_ARRAY_T();

        open catitems_items_cur(catitems_rec.cat_id);
        loop
            fetch catitems_items_cur into catitems_items_rec;
            exit when catitems_items_cur%NOTFOUND;
            var_ci_content_item_array.append(catitems_items_rec.item_id);


        end loop;
        close catitems_items_cur;

        var_ci_content_object.put('contents', var_ci_content_item_array);
        var_ci_contents_array.append(var_ci_content_object);

    end loop;

    var_ci_def_object.put('contents', var_ci_contents_array);

    jo_result.append(var_ci_def_object);


--START ITEM SG CONTENT DEFINITION

    var_isg_def_object := new JSON_OBJECT_T();
    var_isg_def_object.put('type', 'ItemSelectionGroupsDefinition');

    var_isg_contents_array := new JSON_ARRAY_T();

    open sgitems_cur;
    loop
        fetch sgitems_cur into sgitems_rec;
        exit when sgitems_cur%NOTFOUND;

        var_isg_content_object := new JSON_OBJECT_T();
        var_isg_content_object.put('itemID', sgitems_rec.item_id);

        var_isg_content_item_array := new JSON_ARRAY_T();

        open sgitems_sg_cur;
        loop
            fetch sgitems_sg_cur into sgitems_sg_rec;
            exit when sgitems_sg_cur%NOTFOUND;

            if sgitems_sg_rec.item_id = sgitems_rec.item_id then
                var_isg_content_item_array.append(sgitems_sg_rec.selg_id);
            end if;

        end loop;
        close sgitems_sg_cur;

        var_isg_content_object.put('contents', var_isg_content_item_array);
        var_isg_contents_array.append(var_isg_content_object);

    end loop;

    var_isg_def_object.put('contents', var_isg_contents_array);

    jo_result.append(var_isg_def_object);

    res := jo_result.to_clob();

    --insert into json_menu values(sys_guid(), systimestamp, res);

    --WRITE INTO FILESYSTEM
    dpr_clobToFile('menuFile.json', res);

    cur_timestamp := systimestamp;

    open awaiting_cur;
    loop
        fetch awaiting_cur into awaiting_rec;
        exit when awaiting_cur%NOTFOUND;

        write_json_to_file(awaiting_rec.content, awaiting_rec.filename, cur_timestamp);
    end loop;
    close awaiting_cur;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE awaiting_table';

    commit;

    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Exception: SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
end create_json_menu;
/