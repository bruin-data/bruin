{#
  Value pools for the Ashmont datasets.

  Ashmont is invented, and so is every name in it: no real country, city, region
  or person is referenced anywhere in the town. Pool sizes are chosen so that
  names repeat about as often as they do in a real town of this size — several
  residents share a surname, which is a fact the case relies on.
#}

{% set male_first_names = [
  'Alaric', 'Alder', 'Ansel', 'Arlen', 'Barnaby', 'Bertram', 'Caelan', 'Callum',
  'Caspar', 'Cedric', 'Corwin', 'Dorian', 'Dorrell', 'Drystan', 'Eamon', 'Edric',
  'Emmett', 'Everard', 'Ferris', 'Finnian', 'Garrick', 'Gideon', 'Halvard', 'Harlan',
  'Hollis', 'Ivor', 'Jarrett', 'Jorren', 'Kelvin', 'Kemper', 'Lachlan', 'Leland',
  'Lorcan', 'Malden', 'Merrick', 'Milo', 'Nathaniel', 'Norwood', 'Orrin', 'Osric',
  'Padraic', 'Perrin', 'Quillon', 'Quinton', 'Ramsey', 'Redmond', 'Roderick', 'Rowan',
  'Sefton', 'Silas', 'Sorrel', 'Tarquin', 'Thaddeus', 'Torin', 'Ulric', 'Vance',
  'Vidor', 'Warrick', 'Weston', 'Wilder', 'Yorick', 'Zachary', 'Zephyr', 'Balen'
] %}

{% set female_first_names = [
  'Adela', 'Ainsley', 'Alcina', 'Amity', 'Annora', 'Arvela', 'Beatrix', 'Bellamy',
  'Briony', 'Calla', 'Carys', 'Cerise', 'Clemency', 'Cordelia', 'Delphine', 'Dorcas',
  'Edwina', 'Elspeth', 'Emeline', 'Eulalia', 'Faye', 'Fenella', 'Flora', 'Frayda',
  'Gwenna', 'Halla', 'Harriet', 'Hester', 'Ilsa', 'Imogen', 'Isolde', 'Jessamine',
  'Junia', 'Katriona', 'Kestra', 'Lavinia', 'Leonie', 'Linnet', 'Maeve', 'Marisol',
  'Melisande', 'Merryn', 'Nerissa', 'Nolwenn', 'Odette', 'Ophira', 'Perpetua', 'Philippa',
  'Prudence', 'Quilla', 'Rhoswen', 'Romilly', 'Rosalind', 'Saffron', 'Selina', 'Sibyl',
  'Tamsin', 'Thea', 'Tressa', 'Ursula', 'Verity', 'Vivienne', 'Winifred', 'Zerlina'
] %}

{% set surnames = [
  'Ashby', 'Balfour', 'Barrow', 'Bellwether', 'Blackwood', 'Brack', 'Bramble', 'Bramwell',
  'Brightwell', 'Cadwell', 'Calloway', 'Carrick', 'Cassidy', 'Chalmers', 'Corbin', 'Cordray',
  'Cranmer', 'Crowther', 'Culpepper', 'Danforth', 'Darrow', 'Deering', 'Delacourt', 'Dennick',
  'Doverly', 'Dray', 'Dunmore', 'Eastleigh', 'Ebbing', 'Ellery', 'Emberly', 'Everstone',
  'Fallow', 'Farrant', 'Fenwick', 'Ferrin', 'Finchley', 'Flintlock', 'Foxbury', 'Frankley',
  'Gable', 'Garrow', 'Gattis', 'Glaive', 'Godwin', 'Grayling', 'Grimsby', 'Halloway',
  'Hambley', 'Hardacre', 'Harrow', 'Hasket', 'Hathorne', 'Havelock', 'Heddon', 'Hollis',
  'Hulme', 'Ingleby', 'Ironside', 'Jarret', 'Jessop', 'Kaldane', 'Keddle', 'Kessel',
  'Kilburn', 'Kinnaird', 'Lachman', 'Lambourne', 'Larkin', 'Ledbetter', 'Linden', 'Lorn',
  'Lowther', 'Mabry', 'Maddock', 'Mallory', 'Marchmont', 'Marrow', 'Mearns', 'Melrose',
  'Merriwether', 'Millbank', 'Morrow', 'Nadder', 'Nashwell', 'Netherby', 'Norrell', 'Nunnery',
  'Oakbourne', 'Odell', 'Orrick', 'Ostler', 'Padgett', 'Paltry', 'Parlow', 'Pell',
  'Pembrey', 'Penhale', 'Pettifer', 'Prendergast', 'Quarry', 'Quilter', 'Quint', 'Rackham',
  'Radleigh', 'Ravenhill', 'Redgrave', 'Renshaw', 'Ridley', 'Rookwood', 'Rowntree', 'Rushmere',
  'Sallow', 'Sarn', 'Scarsdale', 'Selby', 'Shackleford', 'Sheerwater', 'Shrike', 'Skelton',
  'Slaughter', 'Sowerby', 'Stallard', 'Stannard', 'Strang', 'Swithin', 'Tallowbrook', 'Tamworth',
  'Thackery', 'Thistlewood', 'Tolliver', 'Trencher', 'Trowbridge', 'Tyburn', 'Ulmer', 'Underhill',
  'Vance', 'Varden', 'Vellacott', 'Verity', 'Vickery', 'Volney', 'Wadlow', 'Wakeford',
  'Wardrup', 'Warrender', 'Weatherall', 'Wellbourne', 'Westray', 'Whitlock', 'Wickstead', 'Winterbourne',
  'Withersby', 'Wolstan', 'Woodrow', 'Wrayburn', 'Yarrow', 'Yelverton', 'Zell', 'Zorander',
  'Abernathy', 'Ackroyd', 'Aldington', 'Amberley', 'Anstruther', 'Applewhite', 'Arkwright', 'Attwater'
] %}

{% set districts = [
  'Foundry Quay', 'Kestrel Hill', 'Old Weirs', 'Marlpit', 'Corvid Row',
  'Sallowfield', 'Northgate', 'Tanner Green'
] %}

{% set streets = [
  'Bellrope Walk', 'Cinder Way', 'Cooperage Street', 'Cradle Hill', 'Draper Street',
  'Ember Lane', 'Fettle Street', 'Flax Walk', 'Forge Row', 'Gantry Street',
  'Glasshouse Lane', 'Gravel Rise', 'Hackle Street', 'Halyard Walk', 'Almoner Street',
  'Harrier Close', 'Heddon Street', 'Hollowbank', 'Ironmonger Row', 'Kiln Street',
  'Lamplight Walk', 'Lathe Street', 'Limeburner Lane', 'Longshaw Road', 'Marl Street',
  'Millrace Walk', 'Mortar Lane', 'Nettlebed Road', 'Oakum Street', 'Old Weir Road',
  'Paternoster Walk', 'Pewter Lane', 'Pikestaff Row', 'Quill Street', 'Kestrel Lane',
  'Rampart Road', 'Ratchet Lane', 'Rookery Walk', 'Ropewalk', 'Saltbox Lane',
  'Sawyer Street', 'Scythe Row', 'Shuttle Street', 'Corvid Row', 'Foundry Row',
  'Anvil Street', 'Sluice Lane', 'Smithy Walk', 'Spindle Street', 'Stanchion Road',
  'Stonemason Row', 'Tallow Lane', 'Tanner Street', 'Tollgate Road', 'Threadneedle Walk',
  'Tinderbox Lane', 'Trestle Street', 'Turnstile Walk', 'Vellum Lane', 'Verge Road',
  'Wainwright Street', 'Warp Lane', 'Quarry Rise', 'Weaver Row', 'Wheelwright Walk',
  'Whetstone Street', 'Wickerwork Lane', 'Windlass Road', 'Winnow Street', 'Yardarm Walk',
  'Yoke Lane', 'Bastion Road', 'Beadle Walk', 'Brazier Lane', 'Buckler Street',
  'Candlewick Row', 'Chandler Street', 'Clapper Lane', 'Coppice Road', 'Cordwainer Walk',
  'Curfew Lane', 'Dovetail Street', 'Drawbridge Road', 'Fletcher Row', 'Gauntlet Lane',
  'Grindstone Walk', 'Hoarstone Road', 'Joiner Street', 'Lockkeeper Lane', 'Mangle Walk'
] %}

{% set business_words_a = [
  'Ashmont', 'Bellrope', 'Cinder', 'Coppice', 'Corvid', 'Draper', 'Ember', 'Fettle',
  'Flax', 'Foundry', 'Gantry', 'Glasshouse', 'Halyard', 'Hollowbank', 'Ironmonger', 'Kestrel',
  'Kiln', 'Lamplight', 'Lathe', 'Limeburner', 'Marlpit', 'Millrace', 'Northgate', 'Oakum',
  'Paternoster', 'Pewter', 'Quarry', 'Rampart', 'Rookery', 'Ropewalk', 'Saltbox', 'Sawyer',
  'Sluice', 'Spindle', 'Stanchion', 'Tallow', 'Tanner', 'Threadneedle', 'Tollgate', 'Trestle',
  'Vellum', 'Wainwright', 'Weaver', 'Whetstone', 'Windlass', 'Winnow', 'Yardarm', 'Yoke'
] %}

{% set business_words_b = [
  'Holdings', 'Works', 'Contracts', 'Supply', 'Trading', 'Services', 'Partners', 'Group',
  'Yard', 'Fabrication', 'Haulage', 'Joinery', 'Plant Hire', 'Roofing', 'Surveying', 'Fitters',
  'Provisions', 'Bakery', 'Grocers', 'Outfitters', 'Ironworks', 'Motors', 'Garage', 'Depot',
  'Clinic', 'Chambers', 'Studio', 'Press', 'Laundry', 'Removals'
] %}

{% set vehicle_models = [
  ['Corvid', 'Wren'], ['Corvid', 'Harrier'], ['Corvid', 'Kite'],
  ['Meridian', 'Sable'], ['Meridian', 'Larch'], ['Meridian', 'Foxglove'],
  ['Ostler', 'Tern'], ['Ostler', 'Plover'], ['Ostler', 'Curlew'],
  ['Anvil', 'Drover'], ['Anvil', 'Carter'], ['Anvil', 'Hauler'],
  ['Kestrel', 'Vireo'], ['Kestrel', 'Merlin'], ['Kestrel', 'Kestrel']
] %}

{% set merchant_names = [
  'Tallow Lane Grocers', 'Foundry Row Bakery', 'Kestrel Provisions', 'Marlpit Outfitters',
  'Anvil Street Hardware', 'Cinder Way Fuel', 'Northgate Fuel', 'Tollgate Services',
  'Corvid Row Pharmacy', 'Weaver Row Butchers', 'Sluice Lane Fishmonger', 'Ropewalk Wines',
  'Glasshouse Garden Centre', 'Spindle Street Books', 'Pewter Lane Hardware', 'Quarry Rise Feed',
  'The Winnow Arms', 'The Trestle', 'Millrace Tearooms', 'Paternoster Cafe',
  'Threadneedle Drapers', 'Lathe Street Cycles', 'Oakum Chandlery', 'Saltbox Deli',
  'Kiln Street Pottery', 'Draper Street Chemists', 'Halyard Sports', 'Yardarm Angling',
  'Rookery Newsagent', 'Stanchion Motors', 'Gantry Tyres', 'Vellum Stationers',
  'Bellrope Hairdressers', 'Coppice Nurseries', 'Rampart Launderette', 'Winnow Street Bistro',
  'Flax Walk Florist', 'Ember Lane Bicycles', 'Sawyer Timber', 'Wainwright Carpets'
] %}

{# Trade types, each with the category a card terminal reports under. #}
{% set trade_types = [
  ['Grocers', 'grocery'], ['Provisions', 'grocery'], ['Bakery', 'grocery'],
  ['Butchers', 'grocery'], ['Fishmonger', 'grocery'], ['Deli', 'grocery'],
  ['Fuel', 'fuel'], ['Filling Station', 'fuel'], ['Services', 'fuel'],
  ['Arms', 'hospitality'], ['Tearooms', 'hospitality'], ['Bistro', 'hospitality'],
  ['Cafe', 'hospitality'], ['Tavern', 'hospitality'], ['Kitchen', 'hospitality'],
  ['Outfitters', 'outdoor goods'], ['Field Sports', 'outdoor goods'], ['Angling', 'outdoor goods'],
  ['Chemists', 'pharmacy'], ['Pharmacy', 'pharmacy'],
  ['Hardware', 'hardware'], ['Ironworks', 'hardware'], ['Timber', 'hardware'],
  ['Drapers', 'clothing'], ['Outfitting', 'clothing'], ['Shoes', 'clothing'],
  ['Cycles', 'sport'], ['Sports', 'sport'], ['Leisure', 'sport'],
  ['Motors', 'motor'], ['Tyres', 'motor'], ['Garage', 'motor'],
  ['Books', 'general'], ['Stationers', 'general'], ['Newsagent', 'general'],
  ['Florist', 'general'], ['Launderette', 'services'], ['Hairdressers', 'services'],
  ['Nurseries', 'general'], ['Pottery', 'general']
] %}
